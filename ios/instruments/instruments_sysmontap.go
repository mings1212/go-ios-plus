package instruments

import (
	"fmt"
	"reflect"

	"github.com/danielpaulus/go-ios/ios"
	dtx "github.com/danielpaulus/go-ios/ios/dtx_codec"
	"github.com/danielpaulus/go-ios/ios/golog"
)

type sysmontapMsgDispatcher struct {
	messages chan dtx.Message
}

func newSysmontapMsgDispatcher() *sysmontapMsgDispatcher {
	return &sysmontapMsgDispatcher{make(chan dtx.Message)}
}

func (p *sysmontapMsgDispatcher) Dispatch(m dtx.Message) {
	p.messages <- m
}

const sysmontapName = "com.apple.instruments.server.services.sysmontap"

type sysmontapService struct {
	channel *dtx.Channel
	conn    *dtx.Connection

	deviceInfoService *DeviceInfoService
	msgDispatcher     *sysmontapMsgDispatcher
	procAttrs         []interface{}

	// bundleID filters per-app memory metrics when non-empty; appPID caches
	// the resolved pid between samples and is owned by the reader goroutine.
	bundleID string
	appPID   uint64
}

// NewSysmontapService creates a new sysmontapService
// - samplingInterval is the rate how often to get samples, i.e Xcode's default is 10, which results in sampling output
// each 1 second, with 500 the samples are retrieved every 15 seconds. It doesn't make any correlation between
// the expected rate and the actual rate of samples delivery. We can only conclude, that the lower the rate in digits,
// the faster the samples are delivered
func NewSysmontapService(device ios.DeviceEntry, samplingInterval int) (*sysmontapService, error) {
	deviceInfoService, err := NewDeviceInfoService(device)
	if err != nil {
		return nil, err
	}

	msgDispatcher := newSysmontapMsgDispatcher()
	dtxConn, err := connectInstrumentsWithMsgDispatcher(device, msgDispatcher)
	if err != nil {
		deviceInfoService.Close()
		return nil, err
	}

	processControlChannel := dtxConn.RequestChannelIdentifier(sysmontapName, loggingDispatcher{dtxConn})

	sysAttrs, err := deviceInfoService.systemAttributes()
	if err != nil {
		deviceInfoService.Close()
		dtxConn.Close()
		return nil, err
	}

	procAttrs, err := deviceInfoService.processAttributes()
	if err != nil {
		deviceInfoService.Close()
		dtxConn.Close()
		return nil, err
	}

	config := map[string]interface{}{
		"ur":             samplingInterval,
		"bm":             0,
		"procAttrs":      procAttrs,
		"sysAttrs":       sysAttrs,
		"cpuUsage":       true,
		"physFootprint":  true,
		"sampleInterval": 500000000,
	}
	_, err = processControlChannel.MethodCall("setConfig:", config)
	if err != nil {
		deviceInfoService.Close()
		dtxConn.Close()
		return nil, err
	}

	err = processControlChannel.MethodCallAsync("start")
	if err != nil {
		deviceInfoService.Close()
		dtxConn.Close()
		return nil, err
	}

	return &sysmontapService{processControlChannel, dtxConn, deviceInfoService, msgDispatcher, procAttrs, "", 0}, nil
}

// Close closes up the DTX connection, message dispatcher and dtx.Message channel
func (s *sysmontapService) Close() error {
	close(s.msgDispatcher.messages)

	s.deviceInfoService.Close()
	return s.conn.Close()
}

// ReceiveCPUUsage returns a chan of SysmontapMessage with CPU Usage info
// The method will close the result channel automatically as soon as sysmontapMsgDispatcher's
// dtx.Message channel is closed.
func (s *sysmontapService) ReceiveCPUUsage() chan SysmontapMessage {
	messages := make(chan SysmontapMessage)
	go func() {
		defer close(messages)

		for msg := range s.msgDispatcher.messages {
			sysmontapMessage, err := mapToSysmontapMessage(msg, s.procAttrs)
			if err != nil {
				golog.Debug("expected `sysmontapMessage` from global channel, but received different message", "module", logModule, "message", msg)
				continue
			}

			if s.bundleID != "" {
				s.applyAppBundle(&sysmontapMessage)
			}

			messages <- sysmontapMessage
		}

		golog.Info("sysmontap message dispatcher channel closed", "module", logModule)
	}()

	return messages
}

// SysmontapMessage is a wrapper struct for incoming CPU samples
type SysmontapMessage struct {
	CPUCount       uint64
	EnabledCPUs    uint64
	EndMachAbsTime uint64
	Type           uint64
	SystemCPUUsage CPUUsage
	// Processes holds per-process memory metrics keyed by pid. It is nil when
	// the device did not include process-level data in this sample.
	Processes map[uint64]ProcessMemInfo
	// AppPID and AppPhysFootprint are set only when a bundle id filter is
	// active. AppPID is 0 when the app was not found in this sample.
	AppPID           uint64
	AppPhysFootprint uint64
	// AppCPUUsage is the target app's CPU load in percent of one full core
	// (can exceed 100 for multi-threaded apps). 0 when unavailable.
	AppCPUUsage float64
}

// ProcessMemInfo holds per-process memory metrics in bytes.
type ProcessMemInfo struct {
	PhysFootprint uint64
	MemResident   uint64
	// CPUUsage is the process CPU load in percent of one full core, derived
	// from the "CPU" procAttr's cpuUsage value. 0 when not requested.
	CPUUsage float64
}

type CPUUsage struct {
	CPU_TotalLoad float64
}

// toUint64 converts any numeric type to uint64. Returns false for
// non-numeric types or negative values.
func toUint64(v interface{}) (uint64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i := rv.Int()
		if i < 0 {
			return 0, false
		}
		return uint64(i), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), true
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		if f < 0 {
			return 0, false
		}
		return uint64(f), true
	default:
		return 0, false
	}
}

// toFloat64 converts any numeric type to float64. Returns false for
// non-numeric types.
func toFloat64(v interface{}) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	default:
		return 0, false
	}
}

// requireUint64 extracts a uint64 from a map field, accepting any numeric type.
func requireUint64(m map[string]interface{}, key string) (uint64, error) {
	v, ok := toUint64(m[key])
	if !ok {
		return 0, fmt.Errorf("expected numeric %s, got %T: %+v", key, m[key], m[key])
	}
	return v, nil
}

// requireFloat64 extracts a float64 from a map field, accepting any numeric type.
func requireFloat64(m map[string]interface{}, key string) (float64, error) {
	v, ok := toFloat64(m[key])
	if !ok {
		return 0, fmt.Errorf("expected numeric %s, got %T: %+v", key, m[key], m[key])
	}
	return v, nil
}

// requireMap extracts a map[string]interface{} from a map field.
func requireMap(m map[string]interface{}, key string) (map[string]interface{}, error) {
	raw, exists := m[key]
	if !exists {
		return nil, fmt.Errorf("%s missing in result map", key)
	}
	sub, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[string]interface{} for %s, got %T", key, raw)
	}
	return sub, nil
}

// extractResultMap unwraps the DTX payload into the first result map.
// Payload structure: []interface{ []interface{ map[string]interface{}, ... }, ... }
func extractResultMap(payload []interface{}) (map[string]interface{}, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload in sysmontap message")
	}

	resultArray, ok := payload[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected []interface{} as payload[0], got %T: %+v", payload[0], payload[0])
	}
	if len(resultArray) == 0 {
		return nil, fmt.Errorf("result array is empty in sysmontap payload: %+v", payload)
	}

	resultMap, ok := resultArray[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[string]interface{} for result, got %T: %+v", resultArray[0], resultArray[0])
	}
	return resultMap, nil
}

// SetBundleID enables per-app memory filtering. When set, each sample is
// annotated with the target app's pid (AppPID) and phys footprint
// (AppPhysFootprint). Must be called before ReceiveCPUUsage.
func (s *sysmontapService) SetBundleID(bundleID string) {
	s.bundleID = bundleID
}

// resolveAppPID looks up the pid of the app whose bundle id matches s.bundleID
// via the ProcessList. App processes report their bundle id as Name.
func (s *sysmontapService) resolveAppPID() (uint64, bool) {
	procs, err := s.deviceInfoService.ProcessList()
	if err != nil {
		return 0, false
	}
	for _, p := range procs {
		if p.IsApplication && p.Name == s.bundleID {
			return p.Pid, true
		}
	}
	return 0, false
}

// applyAppBundle annotates the sample with the target app's pid and
// physFootprint. The pid is cached between samples and re-resolved only when
// the app disappears from a sample, so a cold start (which changes the pid) is
// picked up automatically while avoiding a ProcessList call per sample.
func (s *sysmontapService) applyAppBundle(msg *SysmontapMessage) {
	if msg.Processes == nil {
		return
	}
	info, ok := msg.Processes[s.appPID]
	if !ok {
		pid, found := s.resolveAppPID()
		if !found {
			return
		}
		s.appPID = pid
		info, ok = msg.Processes[s.appPID]
		if !ok {
			// The fresh pid is not in this sample yet (race between the two
			// instrument calls); the next sample will pick it up.
			return
		}
	}
	msg.AppPID = s.appPID
	msg.AppPhysFootprint = info.PhysFootprint
	msg.AppCPUUsage = info.CPUUsage
}

// parseProcessCPU extracts the per-process CPU load from the "CPU" procAttr
// value. Instruments returns a dict {"cpuUsage": <fraction of one core>,
// "cpuTotalLoad": <fraction of all cores>}; some iOS versions may send a plain
// number instead. The result is converted to percent of one full core.
func parseProcessCPU(v interface{}) float64 {
	switch m := v.(type) {
	case map[string]interface{}:
		f, _ := toFloat64(m["cpuUsage"])
		return f * 100
	case map[interface{}]interface{}:
		f, _ := toFloat64(m["cpuUsage"])
		return f * 100
	}
	if f, ok := toFloat64(v); ok {
		return f * 100
	}
	return 0
}

// mapToCPUUsage parses a DTX sysmontap message into a SysmontapMessage without
// per-process data. Kept for compatibility with existing callers and tests.
func mapToCPUUsage(msg dtx.Message) (SysmontapMessage, error) {
	return mapToSysmontapMessage(msg, nil)
}

// mapToSysmontapMessage parses a DTX sysmontap message into a SysmontapMessage,
// including per-process memory data when procAttrs is provided.
// It tolerates numeric type variations (int, int64, uint32, float64, etc.)
// across different iOS versions and device types.
func mapToSysmontapMessage(msg dtx.Message, procAttrs []interface{}) (SysmontapMessage, error) {
	resultMap, err := extractResultMap(msg.Payload)
	if err != nil {
		return SysmontapMessage{}, err
	}

	cpuCount, err := requireUint64(resultMap, "CPUCount")
	if err != nil {
		return SysmontapMessage{}, err
	}
	enabledCPUs, err := requireUint64(resultMap, "EnabledCPUs")
	if err != nil {
		return SysmontapMessage{}, err
	}
	endMachAbsTime, err := requireUint64(resultMap, "EndMachAbsTime")
	if err != nil {
		return SysmontapMessage{}, err
	}
	typ, err := requireUint64(resultMap, "Type")
	if err != nil {
		return SysmontapMessage{}, err
	}

	sysCPUMap, err := requireMap(resultMap, "SystemCPUUsage")
	if err != nil {
		return SysmontapMessage{}, err
	}
	cpuTotalLoad, err := requireFloat64(sysCPUMap, "CPU_TotalLoad")
	if err != nil {
		return SysmontapMessage{}, err
	}

	sysmontapMessage := SysmontapMessage{
		CPUCount:       cpuCount,
		EnabledCPUs:    enabledCPUs,
		EndMachAbsTime: endMachAbsTime,
		Type:           typ,
		SystemCPUUsage: CPUUsage{CPU_TotalLoad: cpuTotalLoad},
		Processes:      parseProcesses(resultMap, procAttrs),
	}
	return sysmontapMessage, nil
}

// attrIndex returns the index of name in attrs, or -1 if absent.
func attrIndex(attrs []interface{}, name string) int {
	for i, attr := range attrs {
		if s, ok := attr.(string); ok && s == name {
			return i
		}
	}
	return -1
}

// parseProcesses extracts per-process memory info from the "Processes" dict of
// a sysmontap sample. The device returns a map keyed by pid where each value is
// an array aligned with the procAttrs list requested in setConfig. Returns nil
// when process data is absent or neither footprint attribute was requested.
func parseProcesses(resultMap map[string]interface{}, procAttrs []interface{}) map[uint64]ProcessMemInfo {
	footprintIdx := attrIndex(procAttrs, "physFootprint")
	residentIdx := attrIndex(procAttrs, "memResidentSize")
	cpuIdx := attrIndex(procAttrs, "CPU")
	if footprintIdx < 0 && residentIdx < 0 && cpuIdx < 0 {
		return nil
	}
	raw, exists := resultMap["Processes"]
	if !exists {
		return nil
	}

	processes := map[uint64]ProcessMemInfo{}
	addSample := func(pid interface{}, values interface{}) {
		pidNum, ok := toUint64(pid)
		if !ok {
			return
		}
		attrValues, ok := values.([]interface{})
		if !ok {
			return
		}
		info := ProcessMemInfo{}
		if footprintIdx >= 0 && footprintIdx < len(attrValues) {
			info.PhysFootprint, _ = toUint64(attrValues[footprintIdx])
		}
		if residentIdx >= 0 && residentIdx < len(attrValues) {
			info.MemResident, _ = toUint64(attrValues[residentIdx])
		}
		if cpuIdx >= 0 && cpuIdx < len(attrValues) {
			info.CPUUsage = parseProcessCPU(attrValues[cpuIdx])
		}
		processes[pidNum] = info
	}

	switch procs := raw.(type) {
	case map[string]interface{}:
		for pid, values := range procs {
			addSample(pid, values)
		}
	case map[interface{}]interface{}:
		for pid, values := range procs {
			addSample(pid, values)
		}
	}

	if len(processes) == 0 {
		return nil
	}
	return processes
}
