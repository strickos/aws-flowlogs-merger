package util

/*
DebugLoggingEnabled is a flag that indicates whether or not to print debug logs.
*/
var DebugLoggingEnabled = GetEnvProp("DEBUG_LOGGING", "false") == "true" || GetEnvProp("DEBUG_LOGGING", "false") == "trace"

/*
TraceLoggingEnabled is a flag that indicates whether or not to print Ztrace logs.
*/
var TraceLoggingEnabled = GetEnvProp("DEBUG_LOGGING", "false") == "trace"
