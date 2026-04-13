"""Open a real Chrome app window with a dedicated TCGplayer profile.

This avoids Selenium/webdriver detection during sign-in. After you log in to
TCGplayer in the opened Chrome window and close Chrome, the same profile
directory can be reused by sales_ingester.py via --chrome-user-data-dir.

Usage:
    python3 launch_tcgplayer_profile.py
    python3 launch_tcgplayer_profile.py --user-data-dir .chrome-tcgplayer-auth
"""

import argparse
import subprocess
from pathlib import Path


DEFAULT_USER_DATA_DIR = ".chrome-tcgplayer-auth"
DEFAULT_PROFILE_DIRECTORY = "Default"
LOGIN_URL = "https://www.tcgplayer.com/login"


def main():
    parser = argparse.ArgumentParser(description="Launch a real Chrome window for manual TCGplayer sign-in")
    parser.add_argument("--user-data-dir", default=DEFAULT_USER_DATA_DIR, help="Dedicated Chrome user data dir to create/reuse")
    parser.add_argument("--profile-directory", default=DEFAULT_PROFILE_DIRECTORY, help="Profile directory name inside the user data dir")
    parser.add_argument("--url", default=LOGIN_URL, help="Initial URL to open")
    parser.add_argument("--remote-debugging-port", type=int, default=9222, help="Optional Chrome remote debugging port for later cookie/session export")
    args = parser.parse_args()

    user_data_dir = str(Path(args.user_data_dir).resolve())
    Path(user_data_dir).mkdir(parents=True, exist_ok=True)

    command = [
        "open",
        "-na",
        "Google Chrome",
        "--args",
        f"--user-data-dir={user_data_dir}",
        f"--profile-directory={args.profile_directory}",
    ]
    if args.remote_debugging_port:
        command.append(f"--remote-debugging-port={int(args.remote_debugging_port)}")
    command.extend([
        args.url,
    ])
    subprocess.run(command, check=True)
    print(f"Opened Chrome with profile root: {user_data_dir}")
    print(f"Profile directory: {args.profile_directory}")
    if args.remote_debugging_port:
        print(f"Remote debugging port: {args.remote_debugging_port}")
    print("Sign in to TCGplayer in that Chrome window, then fully quit Chrome when done.")
    print("After that, reuse the same directory with sales_ingester.py --chrome-user-data-dir.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
