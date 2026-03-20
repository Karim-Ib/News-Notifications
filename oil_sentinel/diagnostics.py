"""
oil_sentinel diagnostic CLI.

Usage:
    python inspect.py stats                  # extraction success rates by source
    python inspect.py prompt                 # prompt for the most recent unscored article
    python inspect.py prompt --id 42         # prompt for a specific article ID
    python inspect.py prompt --scored        # include already-scored articles in --id lookup
    python inspect.py prompt --list          # list recent articles you can pick from
"""

import argparse
import sqlite3
import sys
from pathlib import Path

# Ensure unicode box-drawing characters work on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CONFIG_PATH = Path(__file__).parent / "config.ini"
sys.path.insert(0, str(Path(__file__).parent))

# ── Config & DB ───────────────────────────────────────────────────────────────

def _load_db_path() -> str:
    import configparser
    raw = configparser.ConfigParser()
    raw.read(CONFIG_PATH)
    return raw.get("database", "path", fallback="oil_sentinel.db")


def _conn(db_path: str) -> sqlite3.Connection:
    from oil_sentinel.db.models import get_connection
    return get_connection(db_path)


# ── Formatting helpers ────────────────────────────────────────────────────────

W = 72
SEP  = "─" * W
SEP2 = "═" * W


def _header(title: str) -> None:
    print(f"\n{SEP2}")
    print(f"  {title}")
    print(SEP2)


def _section(title: str) -> None:
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)


def _pct(n: int, total: int) -> str:
    return f"{100 * n / total:.1f}%" if total else "n/a"


# ── stats subcommand ──────────────────────────────────────────────────────────

def cmd_stats(db_path: str) -> None:
    conn = _conn(db_path)

    # ── Overall counts ────────────────────────────────────────────────────────
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    with_body = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE body_text IS NOT NULL AND body_text != ''"
    ).fetchone()[0]
    no_body = total - with_body

    pending = conn.execute("SELECT COUNT(*) FROM articles WHERE scored = 0").fetchone()[0]
    scored  = conn.execute("SELECT COUNT(*) FROM articles WHERE scored = 1").fetchone()[0]
    skipped = conn.execute("SELECT COUNT(*) FROM articles WHERE scored = 2").fetchone()[0]

    # Of scored/skipped articles, how many had body text?
    scored_with_body = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE scored IN (1,2) AND body_text IS NOT NULL AND body_text != ''"
    ).fetchone()[0]
    scored_total = scored + skipped

    _header("Oil Sentinel — Extraction Statistics")

    print(f"\nArticles in DB:        {total:>6}")
    print(f"  body_text present:   {with_body:>6}  ({_pct(with_body, total)})")
    print(f"  no body_text:        {no_body:>6}  ({_pct(no_body, total)})")
    print()
    print(f"Scoring status:")
    print(f"  pending (unscored):  {pending:>6}")
    print(f"  scored:              {scored:>6}")
    print(f"  skipped:             {skipped:>6}")
    if scored_total:
        print(f"\nOf scored+skipped articles:")
        print(f"  scored with body:    {scored_with_body:>6}  ({_pct(scored_with_body, scored_total)} had full text)")

    # ── Body text length distribution ─────────────────────────────────────────
    rows = conn.execute(
        "SELECT LENGTH(body_text) FROM articles WHERE body_text IS NOT NULL AND body_text != ''"
    ).fetchall()
    if rows:
        lengths = [r[0] for r in rows]
        _section("Body Text Length Distribution (extracted articles)")
        print(f"  min:     {min(lengths):>6} chars")
        print(f"  median:  {sorted(lengths)[len(lengths)//2]:>6} chars")
        print(f"  max:     {max(lengths):>6} chars")
        print(f"  avg:     {sum(lengths)//len(lengths):>6} chars")

        buckets = [(0, 200, "<200"), (200, 500, "200–499"), (500, 1000, "500–999"),
                   (1000, 1500, "1000–1499"), (1500, 2001, "1500–2000")]
        print()
        for lo, hi, label in buckets:
            count = sum(1 for l in lengths if lo <= l < hi)
            bar = "█" * min(30, count * 30 // max(len(lengths), 1))
            print(f"  {label:<12}  {count:>5}  {bar}")

    # ── Per-source breakdown ───────────────────────────────────────────────────
    rows = conn.execute(
        """
        SELECT
            COALESCE(source_name, '(unknown)') AS src,
            COUNT(*)                            AS total,
            SUM(CASE WHEN body_text IS NOT NULL AND body_text != '' THEN 1 ELSE 0 END) AS extracted,
            AVG(CASE WHEN body_text IS NOT NULL AND body_text != '' THEN LENGTH(body_text) END) AS avg_len
        FROM articles
        GROUP BY src
        HAVING total >= 2
        ORDER BY extracted * 1.0 / total DESC, total DESC
        LIMIT 30
        """
    ).fetchall()

    if rows:
        _section("Per-Source Extraction Rate  (sources with ≥2 articles, sorted by success rate)")
        print(f"  {'Source':<35}  {'Total':>5}  {'Extracted':>9}  {'Rate':>6}  {'Avg len':>7}")
        print(f"  {'-'*35}  {'-'*5}  {'-'*9}  {'-'*6}  {'-'*7}")
        for src, tot, ext, avg_l in rows:
            rate = _pct(ext, tot)
            avg_str = f"{int(avg_l)}" if avg_l else "—"
            indicator = "✓" if ext / tot >= 0.7 else ("~" if ext / tot >= 0.3 else "✗")
            print(f"  {indicator} {src:<33}  {tot:>5}  {ext:>9}  {rate:>6}  {avg_str:>7}")

    # ── Recent failures (last 20 articles with no body_text) ──────────────────
    failures = conn.execute(
        """
        SELECT id, source_name, title, scored
        FROM articles
        WHERE (body_text IS NULL OR body_text = '')
          AND scored IN (1, 2)
        ORDER BY fetched_at DESC
        LIMIT 15
        """
    ).fetchall()

    if failures:
        _section("Recent Scored Articles Without Body Text (extraction failures)")
        for row in failures:
            status = {1: "scored", 2: "skipped"}.get(row["scored"], "?")
            src   = (row["source_name"] or "(unknown)")[:25]
            title = (row["title"] or "(no title)")[:45]
            print(f"  #{row['id']:<5}  [{status}]  {src:<25}  {title}")

    conn.close()
    print()


# ── prompt subcommand ─────────────────────────────────────────────────────────

def _fetch_article(conn: sqlite3.Connection, article_id: int) -> dict | None:
    row = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()
    return dict(row) if row else None


def _fetch_most_recent_unscored(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT * FROM articles WHERE scored = 0 ORDER BY fetched_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def _fetch_recent_list(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, source_name, title, scored, body_text, fetched_at
        FROM articles
        ORDER BY fetched_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def _show_prompt(article: dict, db_path: str) -> None:
    from oil_sentinel.db.models import get_recent_narrative_keys, get_connection as _gc
    from oil_sentinel.scoring.gemini import SYSTEM_PROMPT, _build_prompt

    conn2 = _gc(db_path)
    recent_narratives = get_recent_narrative_keys(conn2, within_hours=12)
    conn2.close()

    body_text = article.get("body_text") or None
    has_body  = bool(body_text)
    user_prompt = _build_prompt(article, recent_narratives, body_text=body_text)

    scored_label = {0: "pending", 1: "scored", 2: "skipped"}.get(article.get("scored"), "?")

    _header(f"Prompt Preview — Article #{article['id']}")
    print(f"\n  Title:    {article.get('title') or '(no title)'}")
    print(f"  Source:   {article.get('source_name') or '(unknown)'}")
    print(f"  Status:   {scored_label}")
    print(f"  Body:     {'YES — ' + str(len(body_text)) + ' chars extracted' if has_body else 'NO  — title-only fallback'}")
    print(f"  Fetched:  {article.get('fetched_at', '')}")

    _section("SYSTEM PROMPT")
    for line in SYSTEM_PROMPT.splitlines():
        print("  " + line)

    _section("USER PROMPT")
    for line in user_prompt.splitlines():
        print("  " + line)

    print(f"\n{SEP}")
    print(f"  Total prompt size: system={len(SYSTEM_PROMPT)} chars  user={len(user_prompt)} chars  total={len(SYSTEM_PROMPT)+len(user_prompt)} chars")
    print(SEP)


def cmd_prompt(db_path: str, article_id: int | None, list_mode: bool) -> None:
    conn = _conn(db_path)

    if list_mode:
        articles = _fetch_recent_list(conn, limit=25)
        conn.close()
        if not articles:
            print("No articles in DB.")
            return
        _header("Recent Articles (run: python diagnostics.py prompt --id <ID>)")
        print(f"\n  {'ID':<6}  {'Status':<8}  {'Body':>4}  {'Source':<28}  Title")
        print(f"  {'─'*6}  {'─'*8}  {'─'*4}  {'─'*28}  {'─'*40}")
        for a in articles:
            status = {0: "pending", 1: "scored", 2: "skipped"}.get(a.get("scored"), "?")
            body_flag = "yes" if a.get("body_text") else " no"
            src   = (a.get("source_name") or "(unknown)")[:28]
            title = (a.get("title") or "(no title)")[:45]
            print(f"  {a['id']:<6}  {status:<8}  {body_flag:>4}  {src:<28}  {title}")
        print()
        return

    if article_id is not None:
        article = _fetch_article(conn, article_id)
        conn.close()
        if not article:
            print(f"No article with id={article_id} found.")
            sys.exit(1)
    else:
        article = _fetch_most_recent_unscored(conn)
        conn.close()
        if not article:
            print("No unscored articles in DB. Use --id <N> or --list to pick one.")
            sys.exit(1)

    _show_prompt(article, db_path)
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="diagnostics.py",
        description="oil_sentinel diagnostics — extraction stats and prompt preview",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("stats", help="Extraction success rates by source")

    p_prompt = sub.add_parser("prompt", help="Preview the Gemini prompt for an article")
    p_prompt.add_argument("--id",     type=int, metavar="N",  help="Article ID to preview")
    p_prompt.add_argument("--list",   action="store_true",    help="List recent articles to pick from")

    args = parser.parse_args()
    db_path = _load_db_path()

    if args.cmd == "stats":
        cmd_stats(db_path)
    elif args.cmd == "prompt":
        cmd_prompt(db_path, article_id=args.id, list_mode=args.list)


if __name__ == "__main__":
    main()
