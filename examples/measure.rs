use ratatui::{
    Frame,
    prelude::Line,
    widgets::{Bar, BarChart, BarGroup, Block, Borders},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        mpsc::{Sender, channel},
    },
    time::{Duration, Instant},
};
use thread_priority::{ThreadBuilder, ThreadPriority};

struct MeasureToolHelper {
    timings: Arc<RwLock<Vec<Duration>>>,
    start_map: HashMap<String, Instant>,
    end_map: HashMap<String, Instant>,
}

impl MeasureToolHelper {
    fn new(timings: Arc<RwLock<Vec<Duration>>>) -> Self {
        Self {
            timings,
            start_map: HashMap::new(),
            end_map: HashMap::new(),
        }
    }

    fn recv_start(&mut self, name: String, instant: Instant) {
        if self.end_map.contains_key(&name) {
            match self.timings.write() {
                Ok(mut timings_raw) => {
                    timings_raw.push(self.end_map.get(&name).unwrap().duration_since(instant));
                    self.end_map.remove(&name);
                }
                Err(e) => {
                    println!("Failed to push to timings after recv new start instant: {e:?}");
                }
            }
        } else {
            self.start_map.insert(name, instant);
        }
    }

    fn recv_end(&mut self, name: String, instant: Instant) {
        if self.start_map.contains_key(&name) {
            match self.timings.write() {
                Ok(mut timings_raw) => {
                    timings_raw.push(instant.duration_since(*self.start_map.get(&name).unwrap()));
                    self.start_map.remove(&name);
                }
                Err(e) => {
                    println!("Failed to push to timings after recv new end instant: {e:?}");
                }
            }
        } else {
            self.end_map.insert(name, instant);
        }
    }

    fn recv_measure_instant(&mut self, measure_instant: MeasureInstant) {
        match measure_instant {
            MeasureInstant::Start { name, instant } => self.recv_start(name, instant),
            MeasureInstant::End { name, instant } => self.recv_end(name, instant),
        }
    }
}

enum MeasureInstant {
    Start { name: String, instant: Instant },
    End { name: String, instant: Instant },
}

pub struct MeasureTool {
    timings: Arc<RwLock<Vec<Duration>>>,
    instant_sender: Sender<MeasureInstant>,
}

impl MeasureTool {
    pub fn new() -> Self {
        let timings = Vec::new();
        let timings_wrapped = Arc::new(RwLock::new(timings));
        let timings_wrapped_clone = timings_wrapped.clone();
        let (tx, rcx) = channel();
        if let Err(e) = ThreadBuilder::default()
            .priority(ThreadPriority::Min)
            .spawn(move |result| {
                if let Err(e) = result {
                    println!("Failed to spawn Measuring Tool thread: {e:?}");
                    return;
                }
                println!("Measuring Tool Thread initialised");
                let mut measure_tool_helper = MeasureToolHelper::new(timings_wrapped_clone);

                loop {
                    match rcx.recv() {
                        Ok(measure_instant) => {
                            measure_tool_helper.recv_measure_instant(measure_instant);
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
            })
        {
            println!("Failed to spawn thread for MeasureTool: {e:?}");
        };

        Self {
            timings: timings_wrapped,
            instant_sender: tx,
        }
    }

    /// Start the clock for the timing measurement tool
    /// - if a previous start time was called and not flushed, it will be overriden (can cause race conditions)
    /// - if no associated end time can be found, will be put in map
    pub fn start_time(&self, name: &str) {
        if let Err(e) = self.instant_sender.send(MeasureInstant::Start {
            name: name.to_string(),
            instant: Instant::now(),
        }) {
            println!("Failed to send MeasureInstant of start time: {e:?}");
        };
    }

    /// Ends the clock for the timing measurement tool
    /// - if a previous end time was called and not flushed, it will be overriden (can cause race conditions)
    /// - if no associated start time can be found, will be put in map
    pub fn end_time(&self, name: &str) {
        if let Err(e) = self.instant_sender.send(MeasureInstant::Start {
            name: name.to_string(),
            instant: Instant::now(),
        }) {
            println!("Failed to send MeasureInstant of start time: {e:?}");
        };
    }

    /// Bins a slice of Durations into `num_bars` equal-width buckets and
    /// returns a ratatui BarChart widget ready to render.
    ///
    /// Bucket width = (max - min) / num_bars, using nanoseconds internally
    /// to avoid floating point drift.
    fn get_histogram<'a>(&self, num_bars: usize, title: &'a str) -> Option<BarChart<'a>> {
        assert!(num_bars > 0, "num_bars must be > 0");
        let durations = {
            match self.timings.read() {
                Ok(timings) => timings,
                Err(e) => {
                    println!("Failed to get read lock on timings Vec: {e:?}");
                    return None;
                }
            }
        };

        if durations.is_empty() {
            return Some(
                BarChart::default().block(Block::default().title(title).borders(Borders::ALL)),
            );
        }

        let min_ns = durations.iter().map(Duration::as_nanos).min().unwrap();
        let max_ns = durations.iter().map(Duration::as_nanos).max().unwrap();

        // Guard against all-equal durations (zero-width range).
        let range = if max_ns == min_ns { 1 } else { max_ns - min_ns };
        let bucket_width = (range as f64 / num_bars as f64).max(1.0);

        let mut counts = vec![0u64; num_bars];
        for d in durations.iter() {
            let ns = d.as_nanos();
            let idx = (((ns - min_ns) as f64) / bucket_width) as usize;
            // clamp: the max value would otherwise land one past the last bucket
            let idx = idx.min(num_bars - 1);
            counts[idx] += 1;
        }

        let bars: Vec<Bar> = counts
            .iter()
            .enumerate()
            .map(|(i, &count)| {
                let bucket_start_ns = min_ns + (i as f64 * bucket_width) as u128;
                let label = format_ns(bucket_start_ns);
                Bar::default()
                    .value(count)
                    .label(Line::from(label))
                    .text_value(count.to_string())
            })
            .collect();

        Some(
            BarChart::default()
                .block(Block::default().title(title).borders(Borders::ALL))
                .data(BarGroup::default().bars(&bars))
                .bar_width(6)
                .bar_gap(1),
        )
    }

    pub fn plot_histogram(&self, frame: &mut Frame, num_bars: usize) {
        let area = frame.area();
        let chart = self.get_histogram(num_bars, "Latency Distribution (press any key to exit)");
        frame.render_widget(chart, area);
    }
}

/// Formats a nanosecond count as a short human-readable label (µs/ms/s).
fn format_ns(ns: u128) -> String {
    if ns < 1_000 {
        format!("{ns}ns")
    } else if ns < 1_000_000 {
        format!("{:.1}µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.1}ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.2}s", ns as f64 / 1_000_000_000.0)
    }
}

fn main() {
    print!("This is used as a helper measurement tool - NOT as a binary!");
}
