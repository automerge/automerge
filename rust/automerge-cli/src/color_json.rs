use std::io::Write;

use serde::Serialize;
use serde_json::ser::Formatter;
use termcolor::{Buffer, BufferWriter, Color, ColorSpec, WriteColor};

struct Style {
    /// style of object brackets
    object_brackets: ColorSpec,
    /// style of array brackets
    array_brackets: ColorSpec,
    /// style of object
    key: ColorSpec,
    /// style of string values
    string_value: ColorSpec,
    /// style of integer values
    integer_value: ColorSpec,
    /// style of float values
    float_value: ColorSpec,
    /// style of bool values
    bool_value: ColorSpec,
    /// style of the `nil` value
    nil_value: ColorSpec,
    /// should the quotation get the style of the inner string/key?
    string_include_quotation: bool,
}

impl Default for Style {
    fn default() -> Self {
        Self {
            object_brackets: ColorSpec::new().set_bold(true).clone(),
            array_brackets: ColorSpec::new().set_bold(true).clone(),
            key: ColorSpec::new()
                .set_fg(Some(Color::Blue))
                .set_bold(true)
                .clone(),
            string_value: ColorSpec::new().set_fg(Some(Color::Green)).clone(),
            integer_value: ColorSpec::new(),
            float_value: ColorSpec::new(),
            bool_value: ColorSpec::new(),
            nil_value: ColorSpec::new(),
            string_include_quotation: true,
        }
    }
}

/// Write pretty printed, colored json to stdout
pub(crate) fn print_colored_json(value: &serde_json::Value) -> std::io::Result<()> {
    let formatter = ColoredFormatter {
        formatter: serde_json::ser::PrettyFormatter::new(),
        style: Style::default(),
        in_object_key: false,
    };
    let mut ignored_writer = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut ignored_writer, formatter);
    value
        .serialize(&mut ser)
        .map_err(|e| std::io::Error::other(e.to_string()))
}

struct ColoredFormatter<F: Formatter> {
    formatter: F,
    style: Style,
    in_object_key: bool,
}

fn write_colored<H>(color: ColorSpec, handler: H) -> std::io::Result<()>
where
    H: FnOnce(&mut Buffer) -> std::io::Result<()>,
{
    let buf = BufferWriter::stdout(termcolor::ColorChoice::Auto);
    let mut buffer = buf.buffer();
    buffer.set_color(&color)?;
    handler(&mut buffer)?;
    buffer.reset()?;
    buf.print(&buffer)?;
    Ok(())
}

impl<F: Formatter> Formatter for ColoredFormatter<F> {
    fn write_null<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.nil_value.clone(), |w| {
            self.formatter.write_null(w)
        })
    }

    fn write_bool<W>(&mut self, _writer: &mut W, value: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.bool_value.clone(), |w| {
            self.formatter.write_bool(w, value)
        })
    }

    fn write_i8<W>(&mut self, _writer: &mut W, value: i8) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_i8(w, value)
        })
    }

    fn write_i16<W>(&mut self, _writer: &mut W, value: i16) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_i16(w, value)
        })
    }

    fn write_i32<W>(&mut self, _writer: &mut W, value: i32) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_i32(w, value)
        })
    }

    fn write_i64<W>(&mut self, _writer: &mut W, value: i64) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_i64(w, value)
        })
    }

    fn write_i128<W>(&mut self, _writer: &mut W, value: i128) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_i128(w, value)
        })
    }

    fn write_u8<W>(&mut self, _writer: &mut W, value: u8) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_u8(w, value)
        })
    }

    fn write_u16<W>(&mut self, _writer: &mut W, value: u16) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_u16(w, value)
        })
    }

    fn write_u32<W>(&mut self, _writer: &mut W, value: u32) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_u32(w, value)
        })
    }

    fn write_u64<W>(&mut self, _writer: &mut W, value: u64) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_u64(w, value)
        })
    }

    fn write_u128<W>(&mut self, _writer: &mut W, value: u128) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_u128(w, value)
        })
    }

    fn write_f32<W>(&mut self, _writer: &mut W, value: f32) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.float_value.clone(), |w| {
            self.formatter.write_f32(w, value)
        })
    }

    fn write_f64<W>(&mut self, _writer: &mut W, value: f64) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.float_value.clone(), |w| {
            self.formatter.write_f64(w, value)
        })
    }

    fn write_number_str<W>(&mut self, _writer: &mut W, value: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.integer_value.clone(), |w| {
            self.formatter.write_number_str(w, value)
        })
    }

    fn begin_string<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let style = if self.style.string_include_quotation {
            if self.in_object_key {
                self.style.key.clone()
            } else {
                self.style.string_value.clone()
            }
        } else {
            ColorSpec::new()
        };
        write_colored(style, |w| self.formatter.begin_string(w))
    }

    fn end_string<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let style = if self.style.string_include_quotation {
            if self.in_object_key {
                self.style.key.clone()
            } else {
                self.style.string_value.clone()
            }
        } else {
            ColorSpec::new()
        };
        write_colored(style, |w| self.formatter.end_string(w))
    }

    fn write_string_fragment<W>(&mut self, _writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let style = if self.in_object_key {
            self.style.key.clone()
        } else {
            self.style.string_value.clone()
        };
        write_colored(style, |w| w.write_all(fragment.as_bytes()))
    }

    fn write_char_escape<W>(
        &mut self,
        _writer: &mut W,
        char_escape: serde_json::ser::CharEscape,
    ) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let style = if self.in_object_key {
            self.style.key.clone()
        } else {
            self.style.string_value.clone()
        };
        write_colored(style, |w| self.formatter.write_char_escape(w, char_escape))
    }

    fn begin_array<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.array_brackets.clone(), |w| {
            self.formatter.begin_array(w)
        })
    }

    fn end_array<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.array_brackets.clone(), |w| {
            self.formatter.end_array(w)
        })
    }

    fn begin_array_value<W>(&mut self, _writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(ColorSpec::new(), |w| {
            self.formatter.begin_array_value(w, first)
        })
    }

    fn end_array_value<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(ColorSpec::new(), |w| self.formatter.end_array_value(w))
    }

    fn begin_object<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.object_brackets.clone(), |w| {
            self.formatter.begin_object(w)
        })
    }

    fn end_object<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(self.style.object_brackets.clone(), |w| {
            self.formatter.end_object(w)
        })
    }

    fn begin_object_key<W>(&mut self, _writer: &mut W, first: bool) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        self.in_object_key = true;
        write_colored(ColorSpec::new(), |w| {
            self.formatter.begin_object_key(w, first)
        })
    }

    fn end_object_key<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        self.in_object_key = false;
        write_colored(ColorSpec::new(), |w| self.formatter.end_object_key(w))
    }

    fn begin_object_value<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        self.in_object_key = false;
        write_colored(ColorSpec::new(), |w| self.formatter.begin_object_value(w))
    }

    fn end_object_value<W>(&mut self, _writer: &mut W) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        self.in_object_key = false;
        write_colored(ColorSpec::new(), |w| self.formatter.end_object_value(w))
    }

    fn write_raw_fragment<W>(&mut self, _writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        write_colored(ColorSpec::new(), |w| {
            self.formatter.write_raw_fragment(w, fragment)
        })
    }
}
