"""
Main pipeline for media monitoring
"""

from datetime import datetime


def main():
    start = datetime.now()
    print("=" * 60)
    print(f"MEDIA MONITOR START | {start}")
    print("=" * 60)

    # TODO:
    # 1. Scrape RSS
    # 2. Classify topics
    # 3. Classify risk / opportunity
    # 4. Export daily report

    end = datetime.now()
    print("=" * 60)
    print(f"PIPELINE END | Duration: {end - start}")
    print("=" * 60)


if __name__ == "__main__":
    main()
