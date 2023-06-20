#[macro_export(local_inner_macros)]
macro_rules! log_miss {
    ($($arg:tt)+) => (log::log!(target: "miss_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_diff {
    ($($arg:tt)+) => (log::log!(target: "diff_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_position {
    ($($arg:tt)+) => (log::log!(target: "position_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_monitor {
    ($($arg:tt)+) => (log::log!(target: "monitor_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_error {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Error, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_info {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Info, $($arg)+));
}

#[macro_export(local_inner_macros)]
macro_rules! log_warn {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Warn, $($arg)+))
}

#[macro_export(local_inner_macros)]
macro_rules! log_debug {
    ($($arg:tt)+) => (log::log!(target: "default_logger", log::Level::Debug, $($arg)+))
}