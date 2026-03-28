#!/usr/bin/env python3
"""
Property Scout - Scrapes German real estate websites for apartments near Fronreute Staig.
Filters: 3 bedrooms, under €350,000, over 80m²
"""

import hashlib
import json
import os
import re
import smtplib
import ssl
from dataclasses import dataclass, asdict
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Configuration - Primary sources first (Kleinanzeigen), then secondary
URLS_PRIMARY = [
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/fronreute/c196l9125",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/weingarten/c196l8271",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/baienfurt/c196l8272",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/baindt/c196l8273",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/schlier/c196l9127",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/berg-ravensburg/c196l9126",
    "https://www.kleinanzeigen.de/s-wohnung-kaufen/koepfingen/c196l9128",
]

URLS_SECONDARY = [
    "https://www.immonet.de/baden-wuerttemberg/ravensburg-fronreute-wohnung-kaufen.html",
    "https://www.immowelt.de/suche/baienfurt/wohnungen/kaufen",
    "https://immonet.de/baden-wuerttemberg/ravensburg-fronreute-blitzenreute-immobilien-kaufen.html",
]

MAX_PRICE = 350000
MIN_SIZE = 80
MIN_ROOMS = 3

GMAIL_USER = os.environ.get("GMAIL_USER")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")

# Multiple recipients: comma-separated in env var, or defaults to GMAIL_USER
_recipients_env = os.environ.get("RECIPIENT_EMAILS", "")
RECIPIENT_EMAILS = [e.strip() for e in _recipients_env.split(",") if e.strip()] or (
    [GMAIL_USER] if GMAIL_USER else []
)

# File to track seen listings (persists between runs)
SEEN_LISTINGS_FILE = Path(__file__).parent / "seen_listings.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}


@dataclass
class Listing:
    title: str
    price: float
    size: float
    rooms: float
    url: str
    source: str
    location: str = ""

    def matches_criteria(self) -> bool:
        # Filter out swap listings and zero-price listings
        if "tausch" in self.title.lower():
            return False
        if self.price <= 0:
            return False
        return (
            self.price <= MAX_PRICE
            and self.size >= MIN_SIZE
            and self.rooms >= MIN_ROOMS
        )

    def listing_id(self) -> str:
        """Generate unique ID based on URL or title+price combo."""
        # Use URL as primary identifier
        return hashlib.md5(self.url.encode()).hexdigest()


def load_seen_listings() -> set[str]:
    """Load previously seen listing IDs from file."""
    if SEEN_LISTINGS_FILE.exists():
        try:
            with open(SEEN_LISTINGS_FILE) as f:
                data = json.load(f)
                return set(data.get("seen_ids", []))
        except (json.JSONDecodeError, IOError):
            return set()
    return set()


def save_seen_listings(seen_ids: set[str]) -> None:
    """Save seen listing IDs to file."""
    with open(SEEN_LISTINGS_FILE, "w") as f:
        json.dump({
            "seen_ids": list(seen_ids),
            "last_updated": datetime.now().isoformat()
        }, f, indent=2)


def extract_number(text: str) -> float:
    """Extract numeric value from text like '€ 299.000' or '85 m²'."""
    if not text:
        return 0.0
    # Remove currency symbols, units, and normalize
    cleaned = re.sub(r"[€m²\s]", "", text)
    # Handle German number format (. as thousand separator, , as decimal)
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def scrape_kleinanzeigen(url: str) -> list[Listing]:
    """Scrape listings from kleinanzeigen.de (primary source)."""
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Kleinanzeigen listing cards
        cards = soup.select("article.aditem, li.ad-listitem, [data-adid]")

        for card in cards:
            try:
                title_elem = card.select_one("a.ellipsis, h2.text-module-begin, .aditem-main--middle--title")
                title = title_elem.get_text(strip=True) if title_elem else "Unknown"

                price_elem = card.select_one(".aditem-main--middle--price-shipping--price, .aditem-main--middle--price, p.aditem-main--middle--price")
                price_text = price_elem.get_text(strip=True) if price_elem else "0"
                # Handle "VB" (Verhandlungsbasis) prices
                if "VB" in price_text:
                    price_text = price_text.replace("VB", "").strip()
                price = extract_number(price_text)

                # Kleinanzeigen often has details in description or tags
                details_elem = card.select_one(".aditem-main--middle--description, .text-module-end")
                details_text = details_elem.get_text(strip=True) if details_elem else ""

                # Try to extract rooms and size from details
                size = 0.0
                rooms = 0.0

                # Look for patterns like "3 Zimmer" or "85 m²"
                size_match = re.search(r"(\d+(?:,\d+)?)\s*m²", details_text)
                if size_match:
                    size = extract_number(size_match.group(1))

                rooms_match = re.search(r"(\d+(?:,\d+)?)\s*(?:Zimmer|Zi\.?|Räume)", details_text, re.IGNORECASE)
                if rooms_match:
                    rooms = extract_number(rooms_match.group(1))

                # Also check tags/attributes
                tags = card.select(".simpletag, .taglist span, .aditem-main--middle--tags span")
                for tag in tags:
                    tag_text = tag.get_text(strip=True)
                    if "m²" in tag_text and size == 0:
                        size = extract_number(tag_text)
                    elif ("Zimmer" in tag_text or "Zi" in tag_text) and rooms == 0:
                        rooms = extract_number(tag_text)

                link_elem = card.select_one("a[href*='/s-anzeige/']")
                listing_url = link_elem["href"] if link_elem else ""
                if listing_url and not listing_url.startswith("http"):
                    listing_url = "https://www.kleinanzeigen.de" + listing_url

                if not listing_url:
                    continue

                location_elem = card.select_one(".aditem-main--top--left, .aditem-location")
                location = location_elem.get_text(strip=True) if location_elem else ""

                listings.append(
                    Listing(
                        title=title,
                        price=price,
                        size=size,
                        rooms=rooms,
                        url=listing_url,
                        source="kleinanzeigen.de",
                        location=location,
                    )
                )
            except Exception as e:
                print(f"Error parsing kleinanzeigen listing: {e}")
                continue

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")

    return listings


def scrape_immonet(url: str) -> list[Listing]:
    """Scrape listings from immonet.de (secondary source)."""
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        cards = soup.select(".listitem, .item, [data-testid='serp-core-classified-card']")

        for card in cards:
            try:
                title_elem = card.select_one("h2, .title, [data-testid='title']")
                title = title_elem.get_text(strip=True) if title_elem else "Unknown"

                price_elem = card.select_one(".price, [data-testid='price']")
                price_text = price_elem.get_text(strip=True) if price_elem else "0"
                price = extract_number(price_text)

                size_elem = card.select_one(".area, [data-testid='area']")
                size_text = size_elem.get_text(strip=True) if size_elem else "0"
                size = extract_number(size_text)

                rooms_elem = card.select_one(".rooms, [data-testid='rooms']")
                rooms_text = rooms_elem.get_text(strip=True) if rooms_elem else "0"
                rooms = extract_number(rooms_text)

                link_elem = card.select_one("a[href]")
                listing_url = link_elem["href"] if link_elem else url
                if listing_url.startswith("/"):
                    listing_url = "https://www.immonet.de" + listing_url

                location_elem = card.select_one(".location, .city, [data-testid='location']")
                location = location_elem.get_text(strip=True) if location_elem else ""

                listings.append(
                    Listing(
                        title=title,
                        price=price,
                        size=size,
                        rooms=rooms,
                        url=listing_url,
                        source="immonet.de",
                        location=location,
                    )
                )
            except Exception as e:
                print(f"Error parsing immonet listing: {e}")
                continue

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")

    return listings


def scrape_immowelt(url: str) -> list[Listing]:
    """Scrape listings from immowelt.de (secondary source)."""
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        cards = soup.select("[data-testid='serp-core-classified-card'], .EstateItem, .listitem_wrap")

        for card in cards:
            try:
                title_elem = card.select_one("h2, .FactsMain, [data-testid='title']")
                title = title_elem.get_text(strip=True) if title_elem else "Unknown"

                price_elem = card.select_one("[data-testid='cardmfe-price-testid'], .price_value, .hardfacts_price")
                price_text = price_elem.get_text(strip=True) if price_elem else "0"
                price = extract_number(price_text)

                facts = card.select(".hardfact, [data-testid='cardmfe-keyfacts-testid'] span")
                size = 0.0
                rooms = 0.0

                for fact in facts:
                    text = fact.get_text(strip=True)
                    if "m²" in text:
                        size = extract_number(text)
                    elif "Zimmer" in text or "Zi" in text:
                        rooms = extract_number(text)

                link_elem = card.select_one("a[href*='/expose/']")
                listing_url = link_elem["href"] if link_elem else url
                if listing_url.startswith("/"):
                    listing_url = "https://www.immowelt.de" + listing_url

                location_elem = card.select_one(".location, [data-testid='cardmfe-description-box-address']")
                location = location_elem.get_text(strip=True) if location_elem else ""

                listings.append(
                    Listing(
                        title=title,
                        price=price,
                        size=size,
                        rooms=rooms,
                        url=listing_url,
                        source="immowelt.de",
                        location=location,
                    )
                )
            except Exception as e:
                print(f"Error parsing immowelt listing: {e}")
                continue

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")

    return listings


def scrape_all_urls() -> list[Listing]:
    """Scrape all configured URLs and return matching listings."""
    all_listings = []

    # Scrape primary sources first (Kleinanzeigen)
    print("\n--- PRIMARY SOURCES (Kleinanzeigen) ---")
    for url in URLS_PRIMARY:
        print(f"Scraping: {url}")
        listings = scrape_kleinanzeigen(url)
        print(f"  Found {len(listings)} raw listings")
        all_listings.extend(listings)

    # Then scrape secondary sources
    print("\n--- SECONDARY SOURCES (Immonet, Immowelt) ---")
    for url in URLS_SECONDARY:
        print(f"Scraping: {url}")
        if "immowelt.de" in url:
            listings = scrape_immowelt(url)
        else:
            listings = scrape_immonet(url)
        print(f"  Found {len(listings)} raw listings")
        all_listings.extend(listings)

    # Filter to matching criteria
    matching = [l for l in all_listings if l.matches_criteria()]

    # Remove duplicates based on URL
    seen_urls = set()
    unique = []
    for listing in matching:
        if listing.url not in seen_urls:
            seen_urls.add(listing.url)
            unique.append(listing)

    return unique


def filter_new_listings(listings: list[Listing]) -> tuple[list[Listing], set[str]]:
    """Filter out previously seen listings, return only new ones."""
    seen_ids = load_seen_listings()
    new_listings = []

    for listing in listings:
        lid = listing.listing_id()
        if lid not in seen_ids:
            new_listings.append(listing)
            seen_ids.add(lid)

    return new_listings, seen_ids


def format_email_html(listings: list[Listing], total_matching: int) -> str:
    """Format listings as HTML email."""
    if not listings:
        return f"""
        <html>
        <body>
        <h2>Property Scout - Weekly Report</h2>
        <p>No <strong>new</strong> apartments matching your criteria this week.</p>
        <p><em>({total_matching} total matching listings exist, but all were previously sent)</em></p>
        <p><strong>Criteria:</strong> 3+ bedrooms, under €350,000, over 80m²</p>
        <p><strong>Locations:</strong> Fronreute, Weingarten, Baienfurt, Baindt, Schlier, Berg, Köpfingen</p>
        </body>
        </html>
        """

    rows = ""
    for l in listings:
        # Highlight primary source
        source_style = "background-color: #e8f5e9; font-weight: bold;" if l.source == "kleinanzeigen.de" else ""
        rows += f"""
        <tr>
            <td style="padding: 10px; border: 1px solid #ddd;">
                <a href="{l.url}">{l.title}</a>
            </td>
            <td style="padding: 10px; border: 1px solid #ddd;">€{l.price:,.0f}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{l.size:.0f} m²</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{l.rooms:.0f}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{l.location}</td>
            <td style="padding: 10px; border: 1px solid #ddd; {source_style}">{l.source}</td>
        </tr>
        """

    return f"""
    <html>
    <body>
    <h2>Property Scout - Weekly Report</h2>
    <p>Found <strong>{len(listings)} NEW</strong> apartments matching your criteria:</p>
    <p><strong>Criteria:</strong> 3+ bedrooms, under €350,000, over 80m²</p>
    <p><strong>Locations:</strong> Fronreute, Weingarten, Baienfurt, Baindt, Schlier, Berg, Köpfingen</p>
    <p><strong>Date:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
    <p><em>Note: Kleinanzeigen listings are highlighted as the primary source.</em></p>

    <table style="border-collapse: collapse; width: 100%; margin-top: 20px;">
        <thead>
            <tr style="background-color: #4CAF50; color: white;">
                <th style="padding: 10px; border: 1px solid #ddd;">Title</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Price</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Size</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Rooms</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Location</th>
                <th style="padding: 10px; border: 1px solid #ddd;">Source</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    </body>
    </html>
    """


def send_email(listings: list[Listing], total_matching: int) -> None:
    """Send email with listings via Gmail SMTP."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        print("ERROR: Gmail credentials not configured.")
        print("Set GMAIL_USER and GMAIL_APP_PASSWORD environment variables.")
        print("\nNew listings found:")
        for l in listings:
            print(f"  - {l.title}: €{l.price:,.0f}, {l.size:.0f}m², {l.rooms:.0f} rooms")
            print(f"    URL: {l.url}")
        return

    if not RECIPIENT_EMAILS:
        print("ERROR: No recipient emails configured.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Property Scout: {len(listings)} NEW apartments - {datetime.now().strftime('%Y-%m-%d')}"
    msg["From"] = GMAIL_USER
    msg["To"] = ", ".join(RECIPIENT_EMAILS)

    html_content = format_email_html(listings, total_matching)
    msg.attach(MIMEText(html_content, "html"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, RECIPIENT_EMAILS, msg.as_string())
        print(f"Email sent successfully to {', '.join(RECIPIENT_EMAILS)}")
    except Exception as e:
        print(f"Failed to send email: {e}")


def main():
    print("=" * 60)
    print("Property Scout - Apartment Search")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Criteria: {MIN_ROOMS}+ rooms, under €{MAX_PRICE:,}, over {MIN_SIZE}m²")
    print("=" * 60)

    # Scrape all sources
    all_matching = scrape_all_urls()
    print(f"\nTotal matching listings: {len(all_matching)}")

    # Filter to only new listings
    new_listings, updated_seen_ids = filter_new_listings(all_matching)
    print(f"New listings (not previously sent): {len(new_listings)}")

    for listing in new_listings:
        print(f"\n  [NEW] {listing.title}")
        print(f"  Price: €{listing.price:,.0f} | Size: {listing.size:.0f}m² | Rooms: {listing.rooms:.0f}")
        print(f"  Location: {listing.location}")
        print(f"  Source: {listing.source}")
        print(f"  URL: {listing.url}")

    # Send email with only new listings
    send_email(new_listings, len(all_matching))

    # Save updated seen listings
    save_seen_listings(updated_seen_ids)
    print(f"\nSaved {len(updated_seen_ids)} listing IDs to {SEEN_LISTINGS_FILE}")

    print("\nDone!")


if __name__ == "__main__":
    main()
