"""Launch a reusable Chrome profile for manual TCGplayer sign-in.

Usage:
    python3 setup_tcgplayer_profile.py
    python3 setup_tcgplayer_profile.py --user-data-dir .chrome-tcgplayer --profile-directory Default

This opens a visible Chrome window on the TCGplayer login page, then waits for
you to finish signing in. After that, the same profile can be reused by
sales_ingester.py for authenticated browser fallback.
"""

import argparse
import shutil
import tempfile
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


DEFAULT_USER_DATA_DIR = ".chrome-tcgplayer"


def make_driver(user_data_dir, profile_directory=None):
    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1400")
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_directory:
        opts.add_argument(f"--profile-directory={profile_directory}")
    return webdriver.Chrome(options=opts)


def clone_profile(source_user_data_dir, source_profile_directory, target_user_data_dir, target_profile_directory):
    source_root = Path(source_user_data_dir).expanduser().resolve()
    source_profile = source_root / source_profile_directory
    target_root = Path(target_user_data_dir).expanduser().resolve()
    target_profile = target_root / target_profile_directory

    if not source_root.exists():
        raise RuntimeError(f"source_user_data_dir_missing:{source_root}")
    if not source_profile.exists():
        raise RuntimeError(f"source_profile_missing:{source_profile}")

    target_root.mkdir(parents=True, exist_ok=True)
    local_state = source_root / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, target_root / "Local State")

    ignore_names = shutil.ignore_patterns(
        "Singleton*",
        "lockfile",
        "LOCK",
        "Crashpad",
        "Crash Reports",
        "Code Cache",
        "GPUCache",
        "DawnCache",
        "ShaderCache",
        "BrowserMetrics",
        "GrShaderCache",
        "GraphiteDawnCache",
        "*.tmp",
    )
    shutil.copytree(source_profile, target_profile, ignore=ignore_names, dirs_exist_ok=True)
    return target_root, target_profile


def main():
    parser = argparse.ArgumentParser(description="Open a reusable Chrome profile for manual TCGplayer login")
    parser.add_argument("--user-data-dir", default=DEFAULT_USER_DATA_DIR, help="Chrome user data directory to create/reuse")
    parser.add_argument("--profile-directory", default="", help="Optional Chrome profile directory name")
    parser.add_argument("--clone-from-user-data-dir", default="", help="Optional existing Chromium user data dir to seed from")
    parser.add_argument("--clone-from-profile-directory", default="Default", help="Profile directory name to copy when seeding")
    args = parser.parse_args()

    user_data_dir = str(Path(args.user_data_dir).resolve())
    profile_directory = args.profile_directory.strip() or None
    target_profile_directory = profile_directory or "Default"

    if args.clone_from_user_data_dir.strip():
        clone_profile(
            args.clone_from_user_data_dir.strip(),
            args.clone_from_profile_directory.strip() or "Default",
            user_data_dir,
            target_profile_directory,
        )

    driver = make_driver(user_data_dir=user_data_dir, profile_directory=profile_directory)
    try:
        driver.get("https://www.tcgplayer.com/login")
        print(f"Chrome profile ready at: {user_data_dir}", flush=True)
        if profile_directory:
            print(f"Profile directory: {profile_directory}", flush=True)
        print("A visible Chrome window should be open on the TCGplayer login page.", flush=True)
        print("Sign in manually, then press Ctrl+C here when you're done.", flush=True)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping profile session.", flush=True)
    finally:
        driver.quit()


if __name__ == "__main__":
    raise SystemExit(main())
