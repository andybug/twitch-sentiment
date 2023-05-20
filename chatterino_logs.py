import datetime
import os
import re
import sys
import typing
import unittest

import pandas as pd


def parse_from_dir(path: str) -> pd.DataFrame:
    """Read all log files in a directory and return a single DataFrame with the combined records

    The log files names' must conform to `stream_name-YYYY-MM-DD.log`.

    Returns a single DataFrame with the following columns:
    - ts: DateTime
    - user: str
    - msg: str
    """

    # add all _files_ in the dir to a list
    files: list[str] = sorted(
        [
            os.path.join(path, f)
            for f in os.listdir(path)
            if os.path.isfile(os.path.join(path, f))
        ]
    )

    frames: list[pd.DataFrame] = []

    # attempt to parse each file and add the DataFrame to the list of frames
    for file in files:
        try:
            df: pd.DataFrame = parse_log(file)
            frames.append(df)
        except FileNameFormatException:
            print(
                f"File '{file}' does not conform to naming scheme. Skipping.",
                file=sys.stderr,
            )
            continue

    # create a single DataFrame from the accumulated frames
    return pd.concat(frames, ignore_index=True)


def parse_log(path: str) -> pd.DataFrame:
    """Attempt to parse the logs from the file at this given path

    The log file name must conform to `stream_name-YYYY-MM-DD.log`.

    Returns a single DataFrame with the following columns:
    - ts: DateTime
    - user: str
    - msg: str
    """
    (_, date) = _file_name_components(path)

    # ts, user, msg
    rows: list[tuple[datetime.datetime, str, str]] = []

    # parse each line in the file and add it to the rows list
    with open(path, "r") as file:
        for line in file:
            try:
                rows.append(_parse_line(date, line.rstrip()))
            except ParseLineException as e:
                print(f"{path}: {e}")
                continue
            except NonMessageLineException:
                # ignore any control messages
                continue

    # combine all rows into a new DataFrame
    return pd.DataFrame(rows, columns=["ts", "user", "msg"])


class ParseLineException(Exception):
    pass


class NonMessageLineException(Exception):
    pass


class _LineComponents(typing.NamedTuple):
    ts: datetime.datetime
    user: str
    msg: str


# normal log line, note the two spaces between the bracket and the username
# [22:26:54]  user: this is the message
_LOG_LINE_PATTERN: typing.Final[typing.Pattern] = re.compile(
    r"^\[(\d{2}:\d{2}:\d{2})\]\s{2}(\w+):\s(.*)$"
)
# announcement line has a single space after the bracket
# capturing the single space so that the match returns something
_ANNOUNCEMENT_LINE_PATTERN: typing.Final[typing.Pattern] = re.compile(
    r"^\[[:0-9]+\](\s{1})\w+.*$"
)
# log comment line starts with #
# capturing the # so that the match returns something
_COMMENT_LINE_PATTERN: typing.Final[typing.Pattern] = re.compile(r"^(#).*$")


def _parse_line(date: datetime.date, line: str) -> _LineComponents:
    """Parse a line into its component parts

    1. timestamp (ts)
    2. username (user)
    3. message (msg)
    """

    match = re.match(_LOG_LINE_PATTERN, line)

    # if it's not a normal log line, try to figure out if it is a different
    # known line format or raise a parse line exception
    if not match:
        if re.match(_ANNOUNCEMENT_LINE_PATTERN, line):
            # announcement line, ignore
            raise NonMessageLineException(line)
        if re.match(_COMMENT_LINE_PATTERN, line):
            # comment, ignore
            raise NonMessageLineException(line)
        else:
            raise ParseLineException(f"Failed to parse line: '{line}'")

    time_str = match.group(1)
    username = match.group(2)
    message = match.group(3)

    assert type(time_str) == str
    assert type(username) == str
    assert type(message) == str

    # combine the HH:MM:SS from the log line with the date passed in to the function to make a datetime
    # the date comes from the log file name
    time_obj = datetime.datetime.strptime(time_str, "%H:%M:%S").time()
    datetime_obj = datetime.datetime.combine(date, time_obj)

    return _LineComponents(datetime_obj, username, message)


class _ParseLineTests(unittest.TestCase):
    def test_ok(self):
        line = "[03:25:34]  user23: lol"
        ts, user, msg = _parse_line(datetime.date(2023, 5, 13), line)

        self.assertEqual(ts, datetime.datetime(2023, 5, 13, 3, 25, 34))
        self.assertEqual(user, "user23")
        self.assertEqual(msg, "lol")

    def test_timestamp_(self):
        line = "[03:61:34]  user23: lol"
        with self.assertRaises(ValueError):
            _parse_line(datetime.date(2023, 5, 13), line)

    def test_announcement(self):
        line = "[17:20:11] Announcement"
        with self.assertRaises(NonMessageLineException):
            _parse_line(datetime.date(2023, 5, 13), line)


class FileNameFormatException(Exception):
    pass


# file names look like: <channel name>-<iso date>.log
# e.g.: summit1g-2023-05-11.log
_FILE_NAME_PATTERN: typing.Final[typing.Pattern] = re.compile(
    r"^(.+?)-(\d{4}-\d{2}-\d{2})\.log$"
)


def _file_name_components(path: str) -> tuple[str, datetime.date]:
    match = re.match(_FILE_NAME_PATTERN, os.path.basename(path))

    if not match:
        raise FileNameFormatException()

    stream_name = match.group(1)
    date_str = match.group(2)

    assert type(stream_name) == str
    assert type(date_str) == str

    date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    return (stream_name, date)


if __name__ == "__main__":
    unittest.main()
