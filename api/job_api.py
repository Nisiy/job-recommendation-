import requests
from bs4 import BeautifulSoup
import time
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ── BongThom RSS ──────────────────────────────────────────
def scrape_bongthom(pages=3):
    jobs = []
    try:
        url = "https://www.bongthom.com/rss.xml"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, "xml")
        for item in soup.find_all("item"):
            jobs.append({
                "source":   "bongthom",
                "title":    item.find("title").get_text(strip=True) if item.find("title") else None,
                "company":  None,
                "location": "Cambodia",
                "deadline": None,
                "url":      item.find("link").get_text(strip=True) if item.find("link") else None,
            })
        print(f"BongThom total: {len(jobs)} jobs")
    except Exception as e:
        print(f"BongThom error: {e}")
    return jobs

# ── CambodiaJobs RSS ──────────────────────────────────────
def scrape_cambodiajobs(pages=3):
    jobs = []
    try:
        url = "https://www.cambodiajobs.biz/feeds/posts/default/-/Cambodia%20Jobs?alt=rss&max-results=50"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, "xml")

        for item in soup.find_all("item"):
            raw_title = item.find("title").get_text(strip=True) if item.find("title") else None
            link      = item.find("link").get_text(strip=True) if item.find("link") else None

            # extract company and deadline from title
            # format: "Position: TITLE (COMPANY)__Deadline:DD-Month"
            company  = None
            deadline = None
            title    = raw_title

            if raw_title:
                # extract company
                company_match = re.search(r'\((.+?)\)', raw_title)
                if company_match:
                    company = company_match.group(1).strip()

                # extract deadline
                deadline_match = re.search(r'Deadline[:\s]*(.+?)$', raw_title)
                if deadline_match:
                    deadline = deadline_match.group(1).strip()

                # clean title — remove company and deadline
                title = re.sub(r'__Deadline.*$', '', raw_title)
                title = re.sub(r'\s*\(.+?\)\s*$', '', title)
                title = re.sub(r'^Position:\s*', '', title).strip()

            jobs.append({
                "source":   "cambodiajobs",
                "title":    title,
                "company":  company,
                "location": "Cambodia",
                "deadline": deadline,
                "url":      link,
            })

        print(f"CambodiaJobs total: {len(jobs)} jobs")
    except Exception as e:
        print(f"CambodiaJobs error: {e}")
    return jobs

# ── KhmerOnlineJobs ───────────────────────────────────────
def scrape_khmeronlinejobs(pages=3):
    jobs = []
    for page in range(1, pages + 1):
        try:
            url = f"https://www.khmeronlinejobs.com/jobs?page={page}"
            response = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(response.content, "html.parser")

            for a in soup.find_all("a", href=True):
                href  = a.get("href", "")
                title = a.get_text(strip=True)
                if "/job/" in href and len(title) > 5:
                    jobs.append({
                        "source":   "khmeronlinejobs",
                        "title":    title,
                        "company":  None,
                        "location": "Cambodia",
                        "deadline": None,
                        "url":      "https://www.khmeronlinejobs.com" + href if href.startswith("/") else href,
                    })
            time.sleep(1)
        except Exception as e:
            print(f"KhmerOnlineJobs error page {page}: {e}")

    # deduplicate
    seen  = set()
    unique = []
    for job in jobs:
        if job["url"] and job["url"] not in seen and job["title"]:
            seen.add(job["url"])
            unique.append(job)

    print(f"KhmerOnlineJobs total: {len(unique)} jobs")
    return unique

# ── Combined ──────────────────────────────────────────────
def fetch_all_jobs(pages=3):
    all_jobs = []
    all_jobs.extend(scrape_bongthom(pages))
    all_jobs.extend(scrape_cambodiajobs(pages))
    all_jobs.extend(scrape_khmeronlinejobs(pages))
    print(f"Total scraped: {len(all_jobs)} jobs")
    return all_jobs