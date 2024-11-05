use std::io::{self, Write};
use tracing_subscriber::fmt::MakeWriter;
use wasm_bindgen::JsValue;
use web_sys::console;

pub struct MakeConsoleWriter;

impl<'a> MakeWriter<'a> for MakeConsoleWriter {
    type Writer = ConsoleWriter;

    fn make_writer(&'a self) -> Self::Writer {
        unimplemented!("use make_writer_for")
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        ConsoleWriter(*meta.level(), Vec::new())
    }
}

pub struct ConsoleWriter(tracing::Level, Vec<u8>);

impl io::Write for ConsoleWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.1.clear();
        let res = self.1.write(buf)?;
        let message = String::from_utf8_lossy(&self.1);
        let message_jsval = JsValue::from_str(&message);
        match self.0 {
            tracing::Level::ERROR => console::error_1(&message_jsval),
            tracing::Level::WARN => console::warn_1(&message_jsval),
            tracing::Level::INFO => console::info_1(&message_jsval),
            tracing::Level::DEBUG => console::debug_1(&message_jsval),
            tracing::Level::TRACE => console::debug_1(&message_jsval),
        }
        //self.1.clear();
        Ok(res)
    }

    fn flush(&mut self) -> io::Result<()> {
        // let message = String::from_utf8_lossy(&self.1);
        // let message_jsval = JsValue::from_str(&message);
        // match self.0 {
        //     tracing::Level::ERROR => console::error_1(&message_jsval),
        //     tracing::Level::WARN => console::warn_1(&message_jsval),
        //     tracing::Level::INFO => console::info_1(&message_jsval),
        //     tracing::Level::DEBUG => console::debug_1(&message_jsval),
        //     tracing::Level::TRACE => console::debug_1(&message_jsval),
        // }
        // self.1.clear();
        Ok(())
    }
}

impl Drop for ConsoleWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
