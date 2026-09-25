use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Paragraph};
use ratatui::Frame;
use syncjoy_protocol::PeerId;

use crate::screen::Screen;

pub(crate) fn draw(frame: &mut Frame, screen: &Screen, id: &PeerId, to: &PeerId, relay: &str) {
    let [status, log, input] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(3),
    ])
    .areas(frame.area());

    let conn = if screen.is_connected() {
        "connected"
    } else {
        "not connected · ctrl-n dials"
    };
    let status_text = format!(" peer {id} → {to} ({conn}) · relay {relay} ");
    frame.render_widget(
        Paragraph::new(status_text).style(Style::default().fg(Color::Cyan)),
        status,
    );

    let messages = screen.messages_snapshot();
    let visible = log.height.saturating_sub(2) as usize;
    let scroll = messages.len().saturating_sub(visible) as u16;
    frame.render_widget(
        Paragraph::new(messages.join("\n"))
            .scroll((scroll, 0))
            .block(Block::bordered().title(" messages ")),
        log,
    );

    let (typed, cursor) = screen.input_view();
    screen.set_input_width(input.width.saturating_sub(4) as usize);
    frame.render_widget(
        Paragraph::new(typed.clone()).block(Block::bordered().title(" input ")),
        input,
    );
    frame.set_cursor_position((input.x + 1 + cursor as u16, input.y + 1));
}
