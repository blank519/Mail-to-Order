"""Inbox monitor: polls an IMAP mailbox for new emails and saves them for the pipeline.

Each new email is written as:
    data/raw/<uid>.eml          -- the complete raw message (headers + body + attachments)
    data/raw/<uid>.meta.json    -- extracted metadata for downstream steps

monitor_state.json tracks a UID "watermark" which indicates the last processed UID.
UIDs are assigned by the server in strictly increasing order, so this marks where to
begin processing new emails.
"""

import json
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv
from imap_tools import MailBox, MailBoxUnencrypted

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("monitor")


def load_config() -> dict:
    """ Load configuration from environment variables."""
    load_dotenv()
    config = {
        "host": os.environ["IMAP_HOST"],
        "port": int(os.environ.get("IMAP_PORT", "993")),
        "user": os.environ["IMAP_USER"],
        "password": os.environ["APP_PASSWORD"],
        "folder": os.environ.get("IMAP_FOLDER", "INBOX"),
        "poll_interval": int(os.environ.get("POLL_INTERVAL", "60")),
        "raw_dir": Path(os.environ.get("RAW_DIR", "data/raw")),
        "state_file": Path(os.environ.get("STATE_FILE", "data/monitor_state.json")),
    }
    #print(config)
    return config


def load_state(state_file: Path) -> dict:
    """ Load the state file, which saves the last processed UID and UID validity."""
    if state_file.exists():
        return json.loads(state_file.read_text())
    return {"uidvalidity": None, "watermark": None}


def save_state(state_file: Path, state: dict) -> None:
    """ Save the state file."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, indent=2))


def process_mailbox(config: dict, state: dict) -> int:
    """
    Fetch emails above the watermark, save the raw message and metadata, return number 
    of emails processed. Updates state to the latest UID.
    """
    host, port = config["host"], config["port"]
    mailbox_cls = MailBox if port == 993 else MailBoxUnencrypted

    processed = 0
    with mailbox_cls(host, port).login(config["user"], config["password"]) as mailbox:
        uidvalidity = mailbox.folder.status(config["folder"], ["UIDVALIDITY"])["UIDVALIDITY"]

        # If the mailbox was rebuilt server-side, old UIDs are invalid: start over.
        if state["uidvalidity"] is not None and state["uidvalidity"] != uidvalidity:
            log.warning("UIDVALIDITY changed (%s -> %s); resetting watermark",
                        state["uidvalidity"], uidvalidity)
            state["watermark"] = None
        state["uidvalidity"] = uidvalidity

        mailbox.folder.set(config["folder"])

        all_uids = mailbox.uids()  # ascending order
        if not all_uids:
            return 0
        max_uid = int(all_uids[-1])

        # First run (or post-rebuild): baseline to "now" so we don't ingest the
        # entire mailbox history. To ingest history instead, set "watermark": 0
        # in the state file.
        if state["watermark"] is None:
            log.warning("First run: baseline set to uid %d; older emails ignored.", max_uid)
            state["watermark"] = max_uid
            return 0

        watermark = state["watermark"]

        # RFC 3501 quirk: "UID N:*" always includes the folder's last message,
        # even when N is past it, so we re-filter client-side as well.
        for msg in mailbox.fetch(f"UID {watermark + 1}:*", mark_seen=False):
            if int(msg.uid) <= watermark:
                continue

            raw_dir = config["raw_dir"]
            raw_dir.mkdir(parents=True, exist_ok=True)

            eml_path = raw_dir / f"{msg.uid}.eml"
            eml_path.write_bytes(msg.obj.as_bytes())

            meta = {
                "uid": msg.uid,
                "message_id": msg.obj["message-id"],
                "subject": msg.subject,
                "from_": msg.from_,
                "to": msg.to,
                "cc": msg.cc,
                "date": msg.date.isoformat() if msg.date else None,
                "attachments": [
                    {"filename": a.filename, "content_type": a.content_type}
                    for a in msg.attachments
                ],
                "raw_path": str(eml_path),
            }
            meta_path = raw_dir / f"{msg.uid}.meta.json"
            meta_path.write_text(json.dumps(meta, indent=2))

            log.info("Saved uid=%s from %s: %s", msg.uid, msg.from_, msg.subject[:60])
            state["watermark"] = max(state["watermark"], int(msg.uid))
            processed += 1

    return processed


def main() -> None:
    """
    Main function to monitor the inbox and process new emails.
    """
    config = load_config()
    state = load_state(config["state_file"])
    log.info("Monitoring %s on %s (folder=%s, poll=%ss)",
             config["user"], config["host"], config["folder"], config["poll_interval"])

    while True:
        try:
            n = process_mailbox(config, state)
            # Saves new state (watermark and uidvalidity) 
            save_state(config["state_file"], state)
            if n:
                log.info("Processed %d new email(s)", n)
            else:
                log.info("No new emails")
        except Exception:
            log.exception("Poll failed; will retry next interval")

        # Polls the mailbox for new emails at an interval
        time.sleep(config["poll_interval"])


if __name__ == "__main__":
    main()
