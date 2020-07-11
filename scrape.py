import re
import json
import dataclasses
import requests
import unicodedata
import psycopg2
from bs4 import BeautifulSoup
from concurrent import futures
from typing import Dict, List

MAX_CONCURRENCY = 20


@dataclasses.dataclass
class Time:
    hour: int
    minute: int


@dataclasses.dataclass
class TimeSpan:
    start: Time
    end: Time


@dataclasses.dataclass
class Course:
    crn: str
    id: str
    term: str
    term_code: str
    subject: str
    subject_code: str
    attributes: List[str]
    title: str
    instructor: str
    credit_hours: str
    time: Dict[str, TimeSpan]
    proj_enr: int
    curr_enr: int
    seats_avail: str
    status: str


def parse_time(course_times):
    times = {}
    pattern = r'(?P<days>[A-Z]+):(?P<start_hour>\d{2})(?P<start_minute>\d{2})\-(?P<end_hour>\d{2})(?P<end_minute>\d{2})'

    for time in [t for t in course_times.split(' ') if t != '']:
        m = re.match(pattern, time)

        if m is None:
            continue

        days = m.group('days')
        start_hour = int(m.group('start_hour'))
        start_minute = int(m.group('start_minute'))
        end_hour = int(m.group('end_hour'))
        end_minute = int(m.group('end_minute'))
        time_span = TimeSpan(
            Time(start_hour, start_minute),
            Time(end_hour, end_minute)
        )

        times.update({day: time_span for day in days})

    return times


def parse_table(soup, term, term_code, subject, subject_code):
    rows = []

    for row in soup.find(id='results').find('table').find('tbody').find_all('tr'):
        cells = row.find_all('td')
        crn = cells[0].find('a').text
        id = cells[1].text
        attributes = [unicodedata.normalize('NFC', a) for a in cells[2].text.split(', ')]
        title = cells[3].text
        instructor = cells[4].text
        credit_hours = cells[5].text  # TODO: parse this
        time = parse_time(cells[6].text)
        proj_enr = int(cells[7].text)
        curr_enr = int(cells[8].text)
        seats_avail = cells[9].text  # TODO: parse this
        status = cells[10].text

        rows.append(Course(
            crn=crn,
            id=id,
            term=term,
            term_code=term_code,
            subject=subject,
            subject_code=subject_code,
            attributes=attributes,
            title=title,
            instructor=instructor,
            credit_hours=credit_hours,
            time=time,
            proj_enr=proj_enr,
            curr_enr=curr_enr,
            seats_avail=seats_avail,
            status=status,
        ))

    return rows


def make_url(term, subject):
    return 'https://courselist.wm.edu/courselist/courseinfo/searchresults' \
           f'?term_code={term}&term_subj={subject}' \
           '&attr=0&attr2=0&levl=0&status=0&ptrm=0&search=Search'


def fetch():
    r = requests.get('https://courselist.wm.edu/courselist/')
    soup = BeautifulSoup(r.text, 'html.parser')

    def parse_select(id_):
        out = []
        for el in soup.find(id=id_):
            if el != "\n" and el['value'] != '0':
                name = el.text
                code = el['value']
                subject_info = {
                    "name": name,
                    "code": code
                }
                out.append(subject_info)
        return out

    terms = parse_select('term_code')
    subjects = parse_select('term_subj')

    with futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        fs = []

        def f(term, subject):
            url = make_url(term['code'], subject['code'])
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')

            # Tables don't have opening <tr> tags, add them for non-empty tables
            if len(soup.find(id='results').find('table').find('tbody').find_all('td')) > 0:
                html_normalized = re.sub(r'<tbody>', '<tbody><tr>', r.text)
                html_normalized = re.sub(r'</tr>\s*<td>', '</tr><tr><td>', html_normalized)
                soup = BeautifulSoup(html_normalized, 'html.parser')

            return parse_table(soup, term['name'], term['code'], subject['name'], subject['code'])

        for term in terms[:1]:
            for subject in subjects[:10]:  # FIXME: remove the bounds
                fs.append(executor.submit(f, term, subject))

        finished, pending = futures.wait(fs, timeout=60, return_when=futures.ALL_COMPLETED)

        if len(pending) > 0:
            raise Exception('some jobs did not complete successfully')

        result_sets = [f.result(timeout=1) for f in finished]

        # flatten the list of courses
        results = [c for result_set in result_sets for c in result_set]

    return results


def list_to_db_array(li):
    li_str = ', '.join([f'"{s}"' for s in li])
    return f'{{{li_str}}}'


def times_to_json(times):
    return json.dumps({day: dataclasses.asdict(time_span) for day, time_span in times.items()})


def course_to_db_insert_query(course: Course):
    c = course
    return f"('{c.crn}', '{c.id}', '{c.term}', '{c.term_code}', '{c.subject}', '{c.subject_code}', "\
           f"'{list_to_db_array(c.attributes)}', '{c.title}', '{c.instructor}', '{c.credit_hours}', " \
           f"'{times_to_json(c.time)}', '{c.proj_enr}', '{c.curr_enr}', '{c.seats_avail}', '{c.status}')"


def build_query(courses: List[Course]):
    courses_query = ', '.join([course_to_db_insert_query(course) for course in courses])
    return f'''
        INSERT INTO courses (
            crn,
            id,
            term,
            term_code,
            subject,
            subject_code,
            attributes,
            title,
            instructor,
            credit_hours,
            time,
            proj_enr,
            curr_enr,
            seats_avail,
            status)
        VALUES
            {courses_query}
    '''


def write_to_db(conn, courses):
    # cursor = conn.cursor()
    query = build_query(courses)
    print(query)


if __name__ == '__main__':
    from pprint import pprint

    #conn = psycopg2.connect("dbname=courses user=postgres")
    courses = fetch()
    write_to_db(1, courses)
