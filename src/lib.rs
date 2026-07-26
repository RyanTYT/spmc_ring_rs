pub mod ring_buffer;

// use crossterm::{
//     event::{self, Event},
//     execute,
//     terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
// };
// use ratatui::{Terminal, backend::CrosstermBackend};
// use std::io::{self, stdout};
//
// fn main() -> io::Result<()> {
//     enable_raw_mode()?;
//     let mut stdout_handle = stdout();
//     execute!(stdout_handle, EnterAlternateScreen)?;
//     let backend = CrosstermBackend::new(stdout_handle);
//     let mut terminal = Terminal::new(backend)?;
//     let measure_tool = MeasureTool::new();
//
//     terminal.draw(|frame| {
//         measure_tool.plot_histogram(frame, 12);
//     })?;
//
//     disable_raw_mode()?;
//     execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
//     terminal.show_cursor()?;
//
//     Ok(())
// }
