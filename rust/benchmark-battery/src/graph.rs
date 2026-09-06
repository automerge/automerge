use crate::format::format_duration;
use anyhow::Context;
use resvg::{tiny_skia, usvg};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

pub struct SeriesLine<'a> {
    pub label: &'static str,
    pub color: &'static str,
    pub points: &'a [u64],
}

pub fn series_graph_path(name: &str) -> PathBuf {
    series_graph_path_with_extension(name, "svg")
}

pub fn series_graph_png_path(name: &str) -> PathBuf {
    series_graph_path_with_extension(name, "png")
}

fn series_graph_path_with_extension(name: &str, extension: &str) -> PathBuf {
    PathBuf::from("series").join(format!("{}.{}", graph_filename(name), extension))
}

pub fn write_series_graph(
    report_dir: &Path,
    name: &str,
    lines: &[SeriesLine<'_>],
) -> anyhow::Result<PathBuf> {
    let svg = series_svg(name, lines);
    let svg_path = write_series_svg(report_dir, name, &svg)?;
    write_series_png(report_dir, name, svg.as_bytes())?;
    Ok(svg_path)
}

pub fn write_series_svg(report_dir: &Path, name: &str, svg: &str) -> anyhow::Result<PathBuf> {
    let relative_path = series_graph_path(name);
    let path = report_dir.join(&relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating graph directory {}", parent.display()))?;
    }
    std::fs::write(&path, svg)
        .with_context(|| format!("writing series graph {}", path.display()))?;
    Ok(relative_path)
}

pub fn write_series_png(report_dir: &Path, name: &str, svg: &[u8]) -> anyhow::Result<PathBuf> {
    let relative_path = series_graph_png_path(name);
    let path = report_dir.join(&relative_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating graph directory {}", parent.display()))?;
    }

    let mut options = usvg::Options::default();
    options.fontdb_mut().load_system_fonts();
    let tree = usvg::Tree::from_data(svg, &options).context("parsing series graph SVG")?;
    let pixmap_size = tree.size().to_int_size();
    let mut pixmap = tiny_skia::Pixmap::new(pixmap_size.width(), pixmap_size.height())
        .context("allocating series graph PNG pixmap")?;
    resvg::render(&tree, tiny_skia::Transform::default(), &mut pixmap.as_mut());
    pixmap
        .save_png(&path)
        .with_context(|| format!("writing series graph PNG {}", path.display()))?;
    Ok(relative_path)
}

fn graph_filename(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn series_svg(name: &str, lines: &[SeriesLine<'_>]) -> String {
    let width = 900.0;
    let height = 320.0;
    let margin_left = 70.0;
    let margin_right = 20.0;
    let margin_top = 35.0;
    let margin_bottom = 45.0;
    let plot_width = width - margin_left - margin_right;
    let plot_height = height - margin_top - margin_bottom;
    let max_len = lines
        .iter()
        .map(|line| line.points.len())
        .max()
        .unwrap_or(0)
        .max(2);
    let max_value = lines
        .iter()
        .flat_map(|line| line.points.iter())
        .copied()
        .max()
        .unwrap_or(1)
        .max(1);

    let mut out = String::new();
    writeln!(
        out,
        r#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img">"#
    )
    .unwrap();
    writeln!(out, "<title>{}</title>", escape_xml(name)).unwrap();
    writeln!(out, r#"<rect width="100%" height="100%" fill="white"/>"#).unwrap();
    writeln!(
        out,
        r#"<text x="{margin_left}" y="22" font-family="sans-serif" font-size="16">{}</text>"#,
        escape_xml(name)
    )
    .unwrap();
    writeln!(
        out,
        r##"<line x1="{margin_left}" y1="{}" x2="{}" y2="{}" stroke="#999"/>"##,
        margin_top + plot_height,
        margin_left + plot_width,
        margin_top + plot_height
    )
    .unwrap();
    writeln!(
        out,
        r##"<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{}" stroke="#999"/>"##,
        margin_top + plot_height
    )
    .unwrap();
    writeln!(
        out,
        r#"<text x="{}" y="{}" text-anchor="end" font-family="sans-serif" font-size="11">{}</text>"#,
        margin_left - 8.0,
        margin_top + 4.0,
        format_duration(max_value)
    )
    .unwrap();
    writeln!(
        out,
        r#"<text x="{}" y="{}" text-anchor="middle" font-family="sans-serif" font-size="11">step</text>"#,
        margin_left + (plot_width / 2.0),
        height - 10.0
    )
    .unwrap();

    let plot = SvgPlot {
        max_len,
        max_value,
        margin_left,
        margin_top,
        width: plot_width,
        height: plot_height,
    };
    for line in lines {
        let points = svg_points(line.points, &plot);
        writeln!(
            out,
            r#"<polyline fill="none" stroke="{}" stroke-width="1.5" points="{}"/>"#,
            line.color, points
        )
        .unwrap();
        if line.points.len() == 1 {
            let (x, y) = svg_point(0, line.points[0], &plot);
            writeln!(
                out,
                r#"<circle cx="{x:.1}" cy="{y:.1}" r="4" fill="{}"/>"#,
                line.color
            )
            .unwrap();
        }
    }

    for (idx, line) in lines.iter().enumerate() {
        let x = width - 165.0 + (idx % 2) as f64 * 85.0;
        let y = 18.0 + (idx / 2) as f64 * 18.0;
        writeln!(
            out,
            r#"<rect x="{x}" y="{y}" width="12" height="3" fill="{}"/><text x="{}" y="{}" font-family="sans-serif" font-size="12">{}</text>"#,
            line.color,
            x + 18.0,
            y + 4.0,
            escape_xml(line.label)
        )
        .unwrap();
    }
    out.push_str("</svg>\n");
    out
}

struct SvgPlot {
    /// Number of steps in the longest series, used to scale point indices across the x-axis.
    max_len: usize,
    /// Largest value in any series, used to scale values across the y-axis.
    max_value: u64,
    /// Horizontal offset from the SVG's left edge to the plot area.
    margin_left: f64,
    /// Vertical offset from the SVG's top edge to the plot area.
    margin_top: f64,
    /// Width of the plot area, excluding space reserved for margins and labels.
    width: f64,
    /// Height of the plot area, excluding space reserved for margins and labels.
    height: f64,
}

fn svg_points(values: &[u64], plot: &SvgPlot) -> String {
    values
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let (x, y) = svg_point(idx, *value, plot);
            format!("{x:.1},{y:.1}")
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// The SVG includes margins for its title, labels, and axes, so data points must be placed
// within the smaller rectangle described by `margin_*` and `plot_*`. The data itself is in
// steps and durations rather than SVG coordinates, so turn `idx` into a fraction of the
// longest series (`max_len`) and `value` into a fraction of the largest duration
// (`max_value`), then scale those fractions to the plot's width and height. The margins
// offset the result into the plot rectangle. Since SVG y coordinates increase downward,
// subtract the scaled value from the bottom of the plot so larger values appear higher.
fn svg_point(idx: usize, value: u64, plot: &SvgPlot) -> (f64, f64) {
    let x = plot.margin_left + ((idx as f64 / (plot.max_len - 1) as f64) * plot.width);
    let y = plot.margin_top + plot.height - ((value as f64 / plot.max_value as f64) * plot.height);
    (x, y)
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
