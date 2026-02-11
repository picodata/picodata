#!/usr/bin/env python
import argparse
import csv
import json
import os
import re
import sys

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, BinaryIO
from urllib.parse import urlencode
from urllib.request import urlopen, Request


def request(url: str, headers: dict[str, str]) -> BinaryIO:
    req = Request(url=url, headers=headers)
    res = urlopen(req)
    assert res.status == 200, res.reason
    return res


def get_json(url: str, headers: dict[str, str]) -> dict[str, Any]:
    res = request(url=url, headers=headers)
    return json.loads(res.read())


def get_pipelines(project_id: int, from_date: datetime, branch: str, headers: dict[str, str]) -> list[dict[str, Any]]:
    result = []
    for page in range(1, 10):
        params = {
            "ref": branch,
            "status": "failed",
            "updated_after": from_date.isoformat(),
            "per_page": 100,
            "page": page,
        }
        url = f"https://git.picodata.io/api/v4/projects/{project_id}/pipelines?{urlencode(params)}"
        pipelines = get_json(url=url, headers=headers)
        if not pipelines:
            break
        result.extend(pipelines)
    print(f"prepare to process {len(result)} failed pipelines", file=sys.stderr)
    return result


def pipeline_test_report(project_id: int, pipeline_id: int, headers: dict[str, str]) -> dict[str, Any]:
    url = f"https://git.picodata.io/api/v4/projects/{project_id}/pipelines/{pipeline_id}/test_report"
    return get_json(url=url, headers=headers)


def failed_tests(args):
    headers = {"PRIVATE-TOKEN": args.gitlab_token}
    from_date = datetime.now() - timedelta(days=args.days)
    pipelines = get_pipelines(project_id=args.project_id, from_date=from_date, branch=args.branch, headers=headers)

    failed_tests = defaultdict(list)
    csv_writer = csv.writer(sys.stdout)

    for pipeline in pipelines:
        print("got failed pipeline", pipeline["web_url"], pipeline["updated_at"], file=sys.stderr)

        test_report = pipeline_test_report(
            project_id=args.project_id,
            pipeline_id=pipeline["id"],
            headers=headers,
        )
        if test_report["failed_count"] == 0:
            continue

        for test_suite in test_report["test_suites"]:
            for test_case in test_suite["test_cases"]:
                if test_case["status"] != "failed":
                    continue
                if args.filter and not re.match(args.filter, test_case["name"]):
                    print("skip failed test", test_case["name"], file=sys.stderr)
                    continue
                print("got failed test", test_case["name"], file=sys.stderr)
                if args.verbose:
                    print(test_case["system_output"], file=sys.stderr)
                failed_tests[test_case["name"]].append({"web_url": pipeline["web_url"], "ref": pipeline["ref"]})

    for test_name, meta in sorted(failed_tests.items(), key=lambda x: len(x[1]), reverse=True):
        if args.csv_format:
            csv_writer.writerow((test_name, meta[0]["web_url"], len(meta)))
        else:
            print(test_name, len(meta))


def parse_args():
    PICODATA_PROJECT_ID = 58

    parser = argparse.ArgumentParser(prog="flaky-finder", description="Looking for flaky tests on master")
    parser.add_argument("-b", "--branch", default="master", help="target branch")
    parser.add_argument("-c", "--csv-format", action=argparse.BooleanOptionalAction, help="print csv with pipeline url")
    parser.add_argument("-d", "--days", type=int, default=30, help="age days count")
    parser.add_argument("-f", "--filter", help="filter tests by regex match with name")
    parser.add_argument("-p", "--project-id", type=int, default=PICODATA_PROJECT_ID, help="GitLab project id")
    parser.add_argument("-v", "--verbose", action=argparse.BooleanOptionalAction, help="print stacktrace to stderr")
    args = parser.parse_args()
    gitlab_token = os.getenv("GITLAB_TOKEN")
    if not gitlab_token:
        raise argparse.ArgumentError(None, "GITLAB_TOKEN env is required")
    args.gitlab_token = gitlab_token
    return args


if __name__ == "__main__":
    args = parse_args()
    failed_tests(args)
