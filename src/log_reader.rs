use std::{
    fs::File,
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
};

use regex::RegexSet;

pub struct Reader {
    path: PathBuf,
    patterns: RegexSet,
    byte_offset: u64,
}

impl Reader {
    pub fn read_latest(&mut self, mut f: impl FnMut(&str)) -> Result<(), std::io::Error> {
        let file = File::open(&self.path)?;
        let new_byte_offset = file.metadata()?.len();

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.byte_offset))?;

        let lines = reader.lines();
        lines.for_each(|line| {
            if let Ok(line) = line
                && self.patterns.matches(&line).matched_any()
            {
                f(&line)
            }
        });

        self.byte_offset = new_byte_offset;

        Ok(())
    }
}

pub fn build(path: impl Into<PathBuf>, patterns: RegexSet) -> Result<Reader, std::io::Error> {
    let mut reader = Reader {
        path: path.into(),
        patterns,
        byte_offset: 0,
    };

    reader.read_latest(|_| {})?;
    Ok(reader)
}
