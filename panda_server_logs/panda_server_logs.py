#!/usr/bin/env python3
"""Weekly report of the error signatures in a PanDA server log, read from
OpenSearch and sent by email.

A signature is "<method> | <error>": the method is the first word of the
message (PanDA's "<method> < <context> > <error>" shape), the error is the
parsed error code (e.g. dbErrorCode) when there is one, otherwise the error
text with quoted values and numbers masked. The context (PandaID, queue) and
the traceback are left out, since they change with every event and every
deployment.

This week is compared with the previous weeks, which are read in the same
query, so nothing is stored between runs:
  new:    not seen in any of the previous weeks
  spike:  this week >= spike_factor x the median of the previous weeks
  always: error code in always_flag
Ignored signatures (ignore file) are excluded from the download, only
counted, and can only be flagged as spike.

Usage:
  python3 panda_server_logs.py -s settings.ini -l dbproxy [--dry-run] [--html report.html] [--replay saved_search.json]
"""

import argparse
import hashlib
import json
import os
import re
import statistics
import sys
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape

from logger import ServiceLogger

# the email library of harvester_service_monitoring, so this runs from the repository checkout
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from harvester_service_monitoring.libs.notifications import Notifications  # noqa: E402

_logger = ServiceLogger("panda_server_logs", __file__).logger
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# the settings of all the monitoring scripts, with the [esserver] account
GENERAL_SETTINGS = os.path.join(os.path.dirname(SCRIPT_DIR), "settings.ini")

# "2026-10-23 08:04:58,182 panda.log.DBProxy: ERROR    " -- the Python logging prefix
HEADER_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ \S+: [A-Z]+\s+")
METHOD_RE = re.compile(r"^(\S+) <")
# error text after the "< context >" part; non-greedy so a ">" in the error isn't taken as the end
AFTER_CONTEXT_RE = re.compile(r"^\S+ <.*? > (.*)$")
TRACEBACK_MARKER = " Traceback (most recent call last)"
# bind variables dumped after the failed SQL ("var={...}", "[{...}, ...]") differ per event
BIND_DUMP_RE = re.compile(r"\s+(?:var=\{|\[\{).*$")
# single-quoted literals are values (site names, attributes), double-quoted text (SQL) is kept
QUOTED_RE = re.compile(r"'[^']*'")
HEX_RE = re.compile(r"0x[0-9a-fA-F]+")
NUMBER_RE = re.compile(r"\d+")
MAX_ERROR_TEXT = 160
# masked error text never contains digits, so an error with digits is a code
CODE_RE = re.compile(r"^[A-Z]+-\d+$")
# the masks left in the error text by QUOTED_RE, HEX_RE and NUMBER_RE
MASK_RE = re.compile(r"'\*'|0xN|(?<![A-Za-z0-9_])N(?![A-Za-z0-9_])")

SOURCE_FIELDS = ["@timestamp", "host.name", "event.original"]
PAGE_SIZE = 1000
# executemany errors dump whole bind arrays on one line
MAX_LINE_CHARS = 990


def signature_of(doc, code_field):
    """Return (signature_id, method, error) for one document"""
    first_line = HEADER_RE.sub("", doc.get("event", {}).get("original", "").split("\n", 1)[0], count=1)
    match = METHOD_RE.match(first_line)
    method = match.group(1) if match else "?"
    code = doc.get(code_field) if code_field else None
    if code:
        error = code
    else:
        match = AFTER_CONTEXT_RE.match(first_line)
        text = match.group(1) if match else first_line
        text = text.split(TRACEBACK_MARKER, 1)[0]
        text = BIND_DUMP_RE.sub("", text).rstrip(": ")
        text = NUMBER_RE.sub("N", HEX_RE.sub("0xN", QUOTED_RE.sub("'*'", text)))
        error = text[:MAX_ERROR_TEXT] or "?"
    return signature_id(method, error), method, error


def signature_id(method, error):
    return hashlib.sha1(f"{method}|{error}".encode("utf-8")).hexdigest()[:8]


def ignore_clause(method, error, code_field):
    """OpenSearch clause matching one signature's documents, or None when it can't be
    expressed as a query (the signature is then dropped after the download).
    The method must be the FIRST token of the message: a plain match would also hit
    errors of other methods whose traceback passes through it."""
    if method == "?":
        return None
    first_token = {"span_first": {"match": {"span_term": {"message": method.lower()}}, "end": 1}}
    if CODE_RE.match(error):
        if not code_field:
            return None
        return {"bool": {"filter": [first_token, {"term": {f"{code_field}.keyword": error}}]}}
    fragment = max((part.strip() for part in MASK_RE.split(error)), key=len, default="")
    if not fragment:
        return None
    clause = {"bool": {"filter": [first_token, {"match_phrase": {"message": fragment}}]}}
    if code_field:
        clause["bool"]["must_not"] = [{"exists": {"field": code_field}}]
    return clause


def load_ignore_rules(path):
    """Return {signature_id: {"method", "error", "reason"}}"""
    parser = ConfigParser(interpolation=None)
    if not parser.read(path):
        raise RuntimeError(f"cannot read ignore file {path}")
    rules = {}
    for section in parser.sections():
        method = parser.get(section, "method")
        error = parser.get(section, "error")
        rules[signature_id(method, error)] = {
            "method": method,
            "error": error,
            "reason": parser.get(section, "reason", fallback=""),
        }
    return rules


def parse_timestamp(value):
    # Python 3.9's fromisoformat doesn't accept "Z"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def period_ranges(end, period, n_periods):
    """[(start, end)] of each period, the current one first"""
    return [(end - period * (i + 1), end - period * i) for i in range(n_periods)]


def es_time(value):
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def build_body(cfg, ranges, clauses):
    date_ranges = {"date_range": {"field": "@timestamp", "ranges": [{"key": str(i), "from": es_time(s), "to": es_time(e)} for i, (s, e) in enumerate(ranges)]}}
    body = {
        "size": min(PAGE_SIZE, cfg["max_documents"]),
        # newest first, so that max_documents cuts the oldest history and not this week
        "sort": [{"@timestamp": "desc"}],
        "_source": SOURCE_FIELDS + ([cfg["code_field"]] if cfg["code_field"] else []),
        "query": {
            "bool": {
                "filter": [
                    {"match": {"fields.type": cfg["log_type"]}},
                    {"match": {"logLevel": cfg["log_level"]}},
                    {"range": {"@timestamp": {"gte": es_time(ranges[-1][0]), "lt": es_time(ranges[0][1])}}},
                ]
            }
        },
        # aggregations see all the documents of the query, including the ignored ones
        "aggs": {"periods": date_ranges, "oldest": {"min": {"field": "@timestamp"}}},
    }
    if clauses:
        body["post_filter"] = {"bool": {"must_not": list(clauses.values())}}
        body["aggs"]["ignored"] = {"filters": {"filters": clauses}, "aggs": {"periods": date_ranges}}
    return body


def connect(settings):
    from opensearchpy import OpenSearch

    return OpenSearch(
        [f"https://{settings.get('esserver', 'host')}/os"],
        http_auth=(settings.get("esserver", "login"), settings.get("esserver", "password")),
        verify_certs=True,
        ca_certs=settings.get("esserver", "capath"),
        timeout=300,
        max_retries=3,
        retry_on_timeout=True,
    )


class SearchResults:
    """Documents of a query, page by page, so that only one page is in memory.
    aggregations is set once the first page is read, truncated once max_documents is hit"""

    def __init__(self, client, cfg, body):
        self.client = client
        self.cfg = cfg
        self.body = body
        self.aggregations = {}
        self.truncated = False

    def pages(self):
        response = self.client.search(index=self.cfg["index"], body=self.body, scroll="5m")
        self.aggregations = response.get("aggregations", {})
        scroll_id = response.get("_scroll_id")
        n_docs = 0
        try:
            while True:
                hits = response["hits"]["hits"]
                if not hits:
                    break
                yield [hit["_source"] for hit in hits]
                n_docs += len(hits)
                if n_docs >= self.cfg["max_documents"]:
                    self.truncated = True
                    break
                response = self.client.scroll(body={"scroll_id": scroll_id, "scroll": "5m"})
                scroll_id = response.get("_scroll_id", scroll_id)
        finally:
            # the scroll context holds resources on a shared cluster until it expires
            if scroll_id:
                try:
                    self.client.clear_scroll(body={"scroll_id": [scroll_id]})
                except Exception as ex:
                    _logger.warning(f"failed to clear the scroll: {ex}")


class SavedResults:
    """A saved _search response (e.g. from curl) instead of a query, for testing"""

    def __init__(self, path):
        with open(path) as f:
            self.data = json.load(f)
        self.aggregations = self.data.get("aggregations", {})
        self.truncated = False

    def newest(self):
        return parse_timestamp(max(hit["_source"]["@timestamp"] for hit in self.data["hits"]["hits"]))

    def pages(self):
        yield [hit["_source"] for hit in self.data["hits"]["hits"]]


def collect(pages, cfg, ranges, rules):
    """Group the documents by signature, counting them per period"""
    end = ranges[0][1]
    period = ranges[0][1] - ranges[0][0]
    n_periods = len(ranges)
    groups = {}
    n_docs = 0
    for page in pages:
        for doc in page:
            n_docs += 1
            index = int((end - parse_timestamp(doc["@timestamp"])) / period)
            if not 0 <= index < n_periods:
                continue
            sig_id, method, error = signature_of(doc, cfg["code_field"])
            group = groups.get(sig_id)
            if group is None:
                group = groups[sig_id] = {
                    "id": sig_id,
                    "method": method,
                    "error": error,
                    "counts": [0] * n_periods,
                    "hosts": set(),
                    "sample": None,
                    "ignored": sig_id in rules,
                }
            group["counts"][index] += 1
            if index == 0:
                group["hosts"].add(doc.get("host", {}).get("name", "?"))
                # documents come newest first: the first one is the latest occurrence, the last one the first
                if group["sample"] is None:
                    group["sample"] = doc.get("event", {}).get("original", "")
                    group["last"] = doc["@timestamp"]
                group["first"] = doc["@timestamp"]
    return groups, n_docs


def add_ignored_counts(groups, aggregations, rules, n_periods):
    """Add the signatures excluded at query time, from their aggregation counts"""
    for sig_id, bucket in aggregations.get("ignored", {}).get("buckets", {}).items():
        counts = [0] * n_periods
        for period_bucket in bucket["periods"]["buckets"]:
            counts[int(period_bucket["key"])] = period_bucket["doc_count"]
        rule = rules[sig_id]
        groups[sig_id] = {
            "id": sig_id,
            "method": rule["method"],
            "error": rule["error"],
            "counts": counts,
            "hosts": set(),
            "sample": None,
            "ignored": True,
        }


def flag(groups, cfg):
    always = {code.strip() for code in cfg["always_flag"].split(",") if code.strip()}
    for group in groups.values():
        current, previous = group["counts"][0], group["counts"][1:]
        group["median"] = statistics.median(previous) if previous else 0
        group["flags"] = []
        if current == 0:
            continue
        if sum(previous) == 0:
            if not group["ignored"]:
                group["flags"].append("new")
        elif current >= cfg["spike_min_count"] and current >= cfg["spike_factor"] * group["median"]:
            group["flags"].append("spike")
        if group["error"] in always and not group["ignored"]:
            group["flags"].append("always")


def time_str(timestamp):
    return timestamp[:19].replace("T", " ")


def sample_text(sample, max_chars):
    lines = [line if len(line) <= MAX_LINE_CHARS else line[:MAX_LINE_CHARS] + " [...]" for line in sample.rstrip("\n").split("\n")]
    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n[... cut]"
    return text


def sample_info(g):
    """(time range, hosts) of a sample"""
    return (
        f"{g['counts'][0]} this week, {time_str(g['first'])} .. {time_str(g['last'])} UTC",
        ", ".join(sorted(name.split(".")[0] for name in g["hosts"])),
    )


def summarize(cfg, groups, ranges, aggregations, truncated, n_docs, rules):
    """The content of the report, shared by the text and the HTML version"""
    period_totals = {int(b["key"]): b["doc_count"] for b in aggregations.get("periods", {}).get("buckets", [])}
    total = period_totals.get(0, sum(g["counts"][0] for g in groups.values()))
    by_count = sorted(groups.values(), key=lambda g: -g["counts"][0])
    active = [g for g in by_count if g["counts"][0] > 0 and not g["ignored"]]
    ignored = [g for g in by_count if g["ignored"]]
    flagged = sorted((g for g in by_count if g["flags"]), key=lambda g: (-len(g["flags"]), -g["counts"][0]))
    start, end = ranges[0]
    level = cfg["log_level"]

    warnings = []
    if truncated:
        warnings.append(f"stopped at max_documents={cfg['max_documents']}, the oldest weeks are incomplete, so 'new' may be wrong")
    oldest = aggregations.get("oldest", {}).get("value_as_string")
    if oldest and parse_timestamp(oldest) > ranges[-1][0] + timedelta(days=1):
        warnings.append(f"OpenSearch only has data since {oldest[:10]}, so 'new' is compared with less history")

    return {
        "subject": f"[PanDA logs] {cfg['title']} {start:%Y-%m-%d} to {end:%Y-%m-%d}: " + (f"{len(flagged)} flagged" if flagged else "nothing new"),
        "title": f"{cfg['title']}: {start:%Y-%m-%d %H:%M} to {end:%Y-%m-%d %H:%M} UTC",
        "facts": [
            f"Source: {cfg['index']} fields.type={cfg['log_type']} logLevel={level}",
            f"This week: {total} {level}s, {len(active)} signatures, {len(ignored)} ignored signatures with {sum(g['counts'][0] for g in ignored)} {level}s",
            f"Documents downloaded: {n_docs}",
        ],
        "warnings": warnings,
        "ranges": ranges,
        "flagged": flagged,
        "active": active,
        "ignored": ignored,
        "rules": rules,
        # the latest occurrence of each signature, flagged ones first
        "samples": [g for g in flagged + [g for g in active if not g["flags"]] if g["sample"]],
        "max_sample_chars": cfg["max_sample_chars"],
    }


NOTHING_FLAGGED = "Nothing new, spiking, or on the always-flag list."
IGNORED_NOTE = "Excluded from the download and only counted, so a spike still shows."


def text_table(header, rows):
    """Plain-text table with aligned columns; the last column (the longest) isn't padded"""
    rows = [[str(value) for value in row] for row in [header] + rows]
    widths = [max(len(row[i]) for row in rows) for i in range(len(header) - 1)]
    return "\n".join("  ".join([value.ljust(width) for value, width in zip(row, widths)] + [row[-1]]) for row in rows)


def signature_table(groups, reasons=None):
    rows = []
    for g in groups:
        description = f"{g['method']} | {g['error']}"
        if reasons:
            description += f"  ({reasons[g['id']]['reason']})"
        previous = " / ".join(str(count) for count in g["counts"][1:])
        rows.append([g["id"], ", ".join(g["flags"]) or "-", g["counts"][0], previous, len(g["hosts"]) or "-", description])
    return text_table(["ID", "Flags", "This week", "Previous", "Hosts", "Method | Error"], rows)


def heading(title):
    return ["", title, "=" * len(title), ""]


def render_text(report):
    lines = [report["title"], ""] + [f"- {fact}" for fact in report["facts"]]
    lines.append(f"- Compared with the previous {len(report['ranges']) - 1} weeks: 'Previous' lists their counts, newest first")
    lines += [f"- WARNING: {warning}" for warning in report["warnings"]]

    flagged, active, ignored = report["flagged"], report["active"], report["ignored"]
    lines += heading(f"Flagged ({len(flagged)})")
    lines.append(signature_table(flagged) if flagged else NOTHING_FLAGGED)
    lines += heading(f"Active signatures ({len(active)})")
    lines.append(signature_table(active) if active else "None.")
    if ignored:
        lines += heading(f"Ignored signatures ({len(ignored)})")
        lines += [IGNORED_NOTE, ""]
        lines.append(signature_table(ignored, report["rules"]))

    lines += heading("Samples")
    for g in report["samples"]:
        title = f"{g['id']} {g['method']} -- {g['error']}" + (f" ({', '.join(g['flags'])})" if g["flags"] else "")
        when, hosts = sample_info(g)
        lines += [title, "-" * min(len(title), 78), when, f"Hosts: {hosts}", "", sample_text(g["sample"], report["max_sample_chars"]), ""]
    return "\n".join(lines)


# inline styles: mail clients (Outlook) drop most of <style>
FLAG_COLORS = {"new": "#c62828", "spike": "#e65100", "always": "#6a1b9a"}
TH_STYLE = "text-align:left;padding:4px 8px;border-bottom:2px solid #999;background:#f0f0f0;white-space:nowrap"
TD_STYLE = "padding:4px 8px;border-bottom:1px solid #ddd;vertical-align:top"
H3_STYLE = "font-size:16px;margin:24px 0 8px;border-bottom:1px solid #ccc;padding-bottom:2px"
PRE_STYLE = (
    "background:#f6f8fa;border:1px solid #ddd;padding:8px;font-size:12px;line-height:1.35;"
    "white-space:pre-wrap;word-wrap:break-word;font-family:Consolas,Menlo,monospace"
)


def badges(flags):
    return " ".join(
        f'<span style="background:{FLAG_COLORS.get(flag, "#555")};color:#fff;border-radius:3px;padding:1px 6px;font-size:12px">{flag}</span>' for flag in flags
    )


def html_table(groups, ranges, linked, reasons=None):
    """Signature table; the IDs with a sample link to it"""

    def td(content, right=False):
        return f'<td style="{TD_STYLE}' + (";text-align:right" if right else "") + f'">{content}</td>'

    header = ["ID", "Flags", "This week"] + [f"{start:%b %d}" for start, _ in ranges[1:]] + ["Hosts", "Method", "Error"] + (["Reason"] if reasons else [])
    rows = ["<tr>" + "".join(f'<th style="{TH_STYLE}">{escape(name)}</th>' for name in header) + "</tr>"]
    for g in groups:
        sig = escape(g["id"])
        if g["id"] in linked:
            sig = f'<a href="#s-{sig}">{sig}</a>'
        cells = [td(f"<code>{sig}</code>"), td(badges(g["flags"])), td(f"<b>{g['counts'][0]}</b>", right=True)]
        cells += [td(count, right=True) for count in g["counts"][1:]]
        cells += [td(len(g["hosts"]) or "-", right=True), td(escape(g["method"])), td(f"<code>{escape(g['error'])}</code>")]
        if reasons:
            cells.append(td(escape(reasons[g["id"]]["reason"])))
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return '<table style="border-collapse:collapse;font-size:13px">' + "".join(rows) + "</table>"


def render_html(report):
    ranges = report["ranges"]
    linked = {g["id"] for g in report["samples"]}
    flagged, active, ignored = report["flagged"], report["active"], report["ignored"]
    facts = report["facts"] + [f"Compared with the previous {len(ranges) - 1} weeks, in the columns named after the day they start"]
    parts = [
        f'<h2 style="font-size:20px;margin:0 0 8px">{escape(report["title"])}</h2>',
        '<ul style="margin:0 0 8px;padding-left:20px">' + "".join(f"<li>{escape(fact)}</li>" for fact in facts) + "</ul>",
    ]
    for warning in report["warnings"]:
        parts.append(f'<div style="background:#fff8e1;border-left:4px solid #f9a825;padding:6px 10px;margin:6px 0"><b>Warning:</b> {escape(warning)}</div>')

    parts.append(f'<h3 style="{H3_STYLE}">Flagged ({len(flagged)})</h3>')
    parts.append(html_table(flagged, ranges, linked) if flagged else f"<p>{NOTHING_FLAGGED}</p>")
    parts.append(f'<h3 style="{H3_STYLE}">Active signatures ({len(active)})</h3>')
    parts.append(html_table(active, ranges, linked) if active else "<p>None.</p>")
    if ignored:
        parts.append(f'<h3 style="{H3_STYLE}">Ignored signatures ({len(ignored)})</h3>')
        parts.append(f'<p style="margin:0 0 8px">{IGNORED_NOTE}</p>')
        parts.append(html_table(ignored, ranges, linked, report["rules"]))

    parts.append(f'<h3 style="{H3_STYLE}">Samples</h3>')
    for g in report["samples"]:
        sig = escape(g["id"])
        when, hosts = sample_info(g)
        parts += [
            # name= for Outlook, id= for the others
            f'<a name="s-{sig}" id="s-{sig}"></a>',
            f'<p style="margin:18px 0 4px;font-size:14px"><code><b>{sig}</b></code> <b>{escape(g["method"])}</b> &mdash; '
            f'<code>{escape(g["error"])}</code> {badges(g["flags"])}</p>',
            f'<p style="margin:0 0 4px;color:#555;font-size:12px">{escape(when)}<br>Hosts: {escape(hosts)}</p>',
            f'<pre style="{PRE_STYLE}">{escape(sample_text(g["sample"], report["max_sample_chars"]))}</pre>',
        ]
    body = "\n".join(parts)
    return (
        '<!DOCTYPE html><html><head><meta charset="utf-8"></head>'
        f'<body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222">\n{body}\n</body></html>'
    )


def send_email(cfg, subject, text, html):
    to = [address.strip() for address in cfg["to"].split(",") if address.strip()]
    if not to:
        raise RuntimeError("no email address in [email] to")
    message = MIMEMultipart("alternative")
    message.attach(MIMEText(text, "plain", "utf-8"))
    message.attach(MIMEText(html, "html", "utf-8"))
    # Notifications writes the text right after its Subject: line, so the MIME headers of the
    # message continue the email headers; the parts are base64, so ASCII with short lines
    Notifications(to=to, mailserver=cfg["mailserver"], fromemail=cfg["from"], text=message.as_string(), subject=subject).send_notification_email()


def read_config(settings, section, settings_path):
    if not settings.has_section(section):
        raise RuntimeError(f"no section [{section}] in {settings_path}")
    get = settings[section]
    # the ignore files are in git next to this script, so a git pull updates them
    ignore_file = get.get("ignore_file", "")
    if ignore_file and not os.path.isabs(ignore_file):
        ignore_file = os.path.join(SCRIPT_DIR, ignore_file)
    return {
        "title": get.get("title", section),
        "index": get.get("index", "atlas_pandalogs-*"),
        "log_type": get["log_type"],
        "log_level": get.get("log_level", "ERROR"),
        "code_field": get.get("code_field", ""),
        "ignore_file": ignore_file,
        "always_flag": get.get("always_flag", ""),
        "period_days": get.getint("period_days", 7),
        "history_periods": get.getint("history_periods", 3),
        "spike_factor": get.getfloat("spike_factor", 5.0),
        "spike_min_count": get.getint("spike_min_count", 50),
        "max_documents": get.getint("max_documents", 200000),
        "max_sample_chars": get.getint("max_sample_chars", 10000),
        # email: [email] defaults, "to" can be set per log
        "to": get.get("to", settings.get("email", "to", fallback="")),
        "mailserver": settings.get("email", "mailserver", fallback="localhost"),
        "from": settings.get("email", "from", fallback="atlpan@mail.cern.ch"),
    }


def main():
    parser = argparse.ArgumentParser(description="Weekly email report of PanDA server log error signatures")
    parser.add_argument("-s", "--settings", required=True, help="settings file")
    parser.add_argument("-l", "--log", required=True, help="section of the settings file with the log to report")
    parser.add_argument("--dry-run", action="store_true", help="print the text version of the email instead of sending it")
    parser.add_argument("--html", help="also write the HTML version of the email to this file, e.g. to look at it in a browser")
    parser.add_argument("--replay", help="read a saved _search response instead of querying OpenSearch")
    args = parser.parse_args()

    try:
        # the general settings first, so that the settings file can override them
        settings = ConfigParser(interpolation=None)
        settings.read(GENERAL_SETTINGS)
        if not settings.read(args.settings):
            raise RuntimeError(f"cannot read settings file {args.settings}")
        cfg = read_config(settings, args.log, args.settings)
        rules = load_ignore_rules(cfg["ignore_file"]) if cfg["ignore_file"] else {}

        period = timedelta(days=cfg["period_days"])
        n_periods = cfg["history_periods"] + 1
        if args.replay:
            results = SavedResults(args.replay)
            # end the current week at the newest document, so that old dumps still fall in it
            ranges = period_ranges(results.newest() + timedelta(seconds=1), period, n_periods)
        else:
            ranges = period_ranges(datetime.now(timezone.utc).replace(microsecond=0), period, n_periods)
            clauses = {}
            for sig_id, rule in rules.items():
                clause = ignore_clause(rule["method"], rule["error"], cfg["code_field"])
                if clause is not None:
                    clauses[sig_id] = clause
            _logger.info(f"querying {cfg['index']} for {args.log}: {es_time(ranges[-1][0])} to {es_time(ranges[0][1])}")
            results = SearchResults(connect(settings), cfg, build_body(cfg, ranges, clauses))

        groups, n_docs = collect(results.pages(), cfg, ranges, rules)
        add_ignored_counts(groups, results.aggregations, rules, n_periods)
        flag(groups, cfg)
        report = summarize(cfg, groups, ranges, results.aggregations, results.truncated, n_docs, rules)
        text, html = render_text(report), render_html(report)
        n_flagged = sum(1 for g in groups.values() if g["flags"])
        _logger.info(f"{args.log}: {n_docs} documents, {len(groups)} signatures, {n_flagged} flagged, truncated={results.truncated}")

        if args.html:
            with open(args.html, "w") as f:
                f.write(html)
        if args.dry_run:
            print(f"Subject: {report['subject']}\n\n{text}")
            return 0
        send_email(cfg, report["subject"], text, html)
        _logger.info(f"{args.log}: sent to {cfg['to']}")
        return 0
    except Exception as ex:
        _logger.exception(f"{args.log}: failed: {ex}")
        print(f"failed: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
