use std::time::Instant;

pub struct Timer {
    start: Option<Instant>,
}

impl Timer {
    pub fn new() -> Self {
        Timer { start: None }
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn elapsed_ms(&self) -> Option<u128> {
        self.start.map(|s| s.elapsed().as_millis())
    }
}

#[macro_export]
macro_rules! time_it {
    ($label:expr, $code:block) => {{
       
        let start = std::time::Instant::now();
        let result = { $code }; // Execute the block of code
        let duration = start.elapsed();
        println!("{}: {:?}", $label.red(), duration);
        result
    }};
}
