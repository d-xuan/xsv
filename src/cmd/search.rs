use csv;
use regex::bytes::RegexBuilder;
use regex::bytes::Regex;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Filters CSV data by whether the given regex matches a row.

The regex is applied to each field in each row, and if any field matches,
then the row is written to the output. The columns to search can be limited
with the '--select' flag (but the full row is still written to the output if
there is a match).

Usage:
    xsv search [options] <regex> [<input>]
    xsv search --help

search options:
    -i, --ignore-case           Case insensitive search. This is equivalent to
                                prefixing the regex with '(?i)'.
    -s, --select <arg>          Select the columns to search. See 'xsv select -h'
                                for the full syntax.
    -v, --invert-match          Select only rows that did not match
    -g, --greater-than          Filter to rows with fields lexigraphically greater
                                than or equal to the argument.
    -l, --less-than             Filter to rows with fields lexigraphically less
                                than or equal to the argument.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_regex: String,
    flag_select: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_invert_match: bool,
    flag_ignore_case: bool,
    flag_greater_than: bool,
    flag_less_than: bool,
}

enum Filter<'a>{
    Eq(Regex),
    Leq(&'a [u8]),
    Geq(&'a [u8]),
}

impl<'a> Filter<'a> {
    fn apply(&self, field: &[u8]) -> bool {
        match self {
            Filter::Eq(pattern) => pattern.is_match(field),
            Filter::Leq(bound) => field <= bound,
            Filter::Geq(bound) => field >= bound,
        }
    }
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let pattern = RegexBuilder::new(&*args.arg_regex)
        .case_insensitive(args.flag_ignore_case)
        .build()?;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    if !rconfig.no_headers {
        wtr.write_record(&headers)?;
    }
    let mut record = csv::ByteRecord::new();

    let filter  = match (args.flag_greater_than, args.flag_less_than) {
        (false, false) => Filter::Eq(pattern),
        (false, true) => Filter::Leq(args.arg_regex.as_bytes()),
        (true, false) => Filter::Geq(args.arg_regex.as_bytes()),
        (true, true) => Filter::Eq(pattern)
    };

    while rdr.read_byte_record(&mut record)? {
        let mut m = sel.select(&record).any(|f| filter.apply(f));
        if args.flag_invert_match {
            m = !m;
        }
        if m {
            wtr.write_byte_record(&record)?;
        }
    }
    Ok(wtr.flush()?)
}
