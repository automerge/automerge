use automerge as am;
use combine::{parser::char as charparser, EasyParser, ParseError, Parser};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChangeError {
    #[error("Invalid change script: {message}")]
    InvalidChangeScript { message: String },
    #[error("Error reading changes: {:?}", source)]
    ErrReadingChanges {
        #[source]
        source: std::io::Error,
    },
    #[error("Error loading changes: {:?}", source)]
    ErrApplyingInitialChanges {
        #[source]
        source: am::AutomergeError,
    },
    #[error("Error writing changes to output file: {:?}", source)]
    ErrWritingChanges {
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug)]
enum Op {
    Set,
    Insert,
    Delete,
    Increment,
}

fn case_insensitive_string<Input>(s: &'static str) -> impl Parser<Input, Output = String>
where
    Input: combine::Stream<Token = char>,
    Input::Error: combine::ParseError<Input::Token, Input::Range, Input::Position>,
{
    charparser::string_cmp(s, |l, r| l.eq_ignore_ascii_case(&r)).map(|s| s.to_lowercase())
}

fn op_parser<Input>() -> impl combine::Parser<Input, Output = Op>
where
    Input: combine::Stream<Token = char>,
{
    combine::choice((
        combine::attempt(case_insensitive_string("set")).map(|_| Op::Set),
        combine::attempt(case_insensitive_string("insert")).map(|_| Op::Insert),
        combine::attempt(case_insensitive_string("delete")).map(|_| Op::Delete),
        combine::attempt(case_insensitive_string("increment")).map(|_| Op::Increment),
    ))
}

fn key_parser<Input>() -> impl Parser<Input, Output = String>
where
    Input: combine::Stream<Token = char>,
{
    let key_char_parser = combine::choice::<Input, _>((
        charparser::alpha_num(),
        charparser::char('-'),
        charparser::char('_'),
    ));
    combine::many1(key_char_parser).map(|chars: Vec<char>| chars.into_iter().collect())
}

fn index_parser<Input>() -> impl Parser<Input, Output = u32>
where
    Input: combine::Stream<Token = char>,
{
    combine::many1::<Vec<char>, Input, _>(charparser::digit()).map(|digits| {
        let num_string: String = digits.iter().collect();
        num_string.parse::<u32>().unwrap()
    })
}

combine::parser! {
    fn path_segment_parser[Input](path_so_far: amf::Path)(Input) -> amf::Path
    where [Input:  combine::Stream<Token=char>]
    {
        let key_path_so_far = path_so_far.clone();
        let key_segment_parser = charparser::string("[\"")
            .with(key_parser())
            .skip(charparser::string("\"]"))
            .then(move |key| path_segment_parser(key_path_so_far.clone().key(key)));

        let index_path_so_far = path_so_far.clone();
        let index_segment_parser = charparser::char('[')
            .with(index_parser())
            .skip(charparser::char(']'))
            .then(move |index| path_segment_parser(index_path_so_far.clone().index(index)));

        combine::choice((
            combine::attempt(key_segment_parser),
            combine::attempt(index_segment_parser),
            combine::value(path_so_far.clone())
        ))
    }
}

fn value_parser<'a, Input>(
) -> Box<dyn combine::Parser<Input, Output = amf::Value, PartialState = ()> + 'a>
where
    Input: 'a,
    Input: combine::Stream<Token = char>,
    Input::Error: combine::ParseError<Input::Token, Input::Range, Input::Position>,
{
    combine::parser::combinator::no_partial(
        //combine::position().and(combine::many1::<Vec<char>, _, _>(combine::any())).and_then(
        combine::position().and(combine::many1::<Vec<char>, _, _>(combine::any())).flat_map(
        |(position, chars): (Input::Position, Vec<char>)| -> Result<amf::Value, Input::Error> {
            let json_str: String = chars.into_iter().collect();
            let json: serde_json::Value = serde_json::from_str(json_str.as_str()).map_err(|e| {
                //let pe = <Input::Error as ParseError<_, _, _>>::StreamError::message::<combine::error::Format<String>>(combine::error::Format(e.to_string()));
                //let pe = <Input::Error as ParseError<Input::Token, Input::Range, Input::Position>>::StreamError::message(e.to_string().into());
                let mut pe = Input::Error::empty(position);
                pe.add_message(combine::error::Format(e.to_string()));
                //let pe = combine::ParseError:::wmpty(position);
                pe
            })?;
            Ok(amf::Value::from_json(&json))
        },
        )
    ).boxed()
}

fn change_parser<'a, Input: 'a>() -> impl combine::Parser<Input, Output = amf::LocalChange> + 'a
where
    Input: 'a,
    Input: combine::stream::Stream<Token = char>,
    Input::Error: combine::ParseError<Input::Token, Input::Range, Input::Position>,
{
    charparser::spaces()
        .with(
            op_parser()
                .skip(charparser::spaces())
                .skip(charparser::string("$"))
                .and(path_segment_parser(am::Path::root())),
        )
        .skip(charparser::spaces())
        .then(|(operation, path)| {
            let onwards: Box<
                dyn combine::Parser<Input, Output = amf::LocalChange, PartialState = _>,
            > = match operation {
                Op::Set => value_parser::<'a>()
                    .map(move |value| amf::LocalChange::set(path.clone(), value))
                    .boxed(),
                Op::Insert => value_parser::<'a>()
                    .map(move |value| amf::LocalChange::insert(path.clone(), value))
                    .boxed(),
                Op::Delete => combine::value(amf::LocalChange::delete(path)).boxed(),
                Op::Increment => combine::value(amf::LocalChange::increment(path)).boxed(),
            };
            onwards
        })
}

fn parse_change_script(input: &str) -> Result<amf::LocalChange, ChangeError> {
    let (change, _) =
        change_parser()
            .easy_parse(input)
            .map_err(|e| ChangeError::InvalidChangeScript {
                message: e.to_string(),
            })?;
    Ok(change)
}

pub fn change(
    mut reader: impl std::io::Read,
    mut writer: impl std::io::Write,
    script: &str,
) -> Result<(), ChangeError> {
    let mut buf: Vec<u8> = Vec::new();
    reader
        .read_to_end(&mut buf)
        .map_err(|e| ChangeError::ErrReadingChanges { source: e })?;
    let backend = am::Automerge::load(&buf)
        .map_err(|e| ChangeError::ErrApplyingInitialChanges { source: e })?;
    let local_change = parse_change_script(script)?;
    let ((), new_changes) = frontend.change::<_, _, amf::InvalidChangeRequest>(None, |d| {
        d.add_change(local_change)?;
        Ok(())
    })?;
    let change_bytes = backend.save().unwrap();
    writer
        .write_all(&change_bytes)
        .map_err(|e| ChangeError::ErrWritingChanges { source: e })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;

    #[test]
    fn test_parse_change_script() {
        struct Scenario {
            input: &'static str,
            expected: amf::LocalChange,
        }
        let scenarios = vec![
            Scenario {
                input: "set $[\"map\"][0] {\"some\": \"value\"}",
                expected: amf::LocalChange::set(
                    amf::Path::root().key("map").index(0),
                    amf::Value::from(hashmap! {"some" => "value"}),
                ),
            },
            Scenario {
                input: "insert $[\"map\"][0] {\"some\": \"value\"}",
                expected: amf::LocalChange::insert(
                    amf::Path::root().key("map").index(0),
                    hashmap! {"some" => "value"}.into(),
                ),
            },
            Scenario {
                input: "delete $[\"map\"][0]",
                expected: amf::LocalChange::delete(amf::Path::root().key("map").index(0)),
            },
            Scenario {
                input: "increment $[\"map\"][0]",
                expected: amf::LocalChange::increment(amf::Path::root().key("map").index(0)),
            },
        ];
        for (index, scenario) in scenarios.into_iter().enumerate() {
            let result: Result<(amf::LocalChange, _), _> =
                change_parser().easy_parse(scenario.input);
            let change = result.unwrap().0;
            assert_eq!(
                change,
                scenario.expected,
                "Failed on scenario: {0}",
                index + 1,
            );
        }
    }
}
