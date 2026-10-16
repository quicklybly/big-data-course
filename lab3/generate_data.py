"""
Hogwarts house points ledger.

python3 generate_data.py                      # ~1.1 ГБ в data/events.csv
python3 generate_data.py --size-mb 50 --out /tmp/sample.csv``
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta

START_DATE = datetime(2026, 9, 1)
DAYS = 3
BATCH = 100_000

COLUMNS = [
    "incident_id",
    "created_at",
    "created_date",
    "student_name",
    "house",
    "school_year",
    "professor",
    "points_delta",
    "reason",
    "location",
    "spell_cast",
    "spell_success",
    "wand_wood",
    "wand_core",
    "creature_involved",
    "damage_galleons",
    "detention_hours",
    "witnesses_count",
    "peeves_involved",
    "filch_anger_level",
    "owl_sent_to_parents",
    "howler_received",
]

HOUSES = ["Gryffindor", "Slytherin", "Ravenclaw", "Hufflepuff"]

FIRST_NAMES = [
    "Harry", "Hermione", "Ron", "Neville", "Luna", "Ginny", "Draco", "Cedric",
    "Cho", "Seamus", "Dean", "Fred", "George", "Lavender", "Padma", "Parvati",
    "Vincent", "Gregory", "Pansy", "Ernie", "Hannah", "Justin", "Colin", "Dennis",
    "Romilda", "Cormac", "Zacharias", "Marcus", "Oliver", "Katie", "Angelina", "Lee",
    "Millicent", "Theodore", "Blaise", "Susan", "Terry", "Michael", "Anthony", "Moaning",
]

LAST_NAMES = [
    "Potter", "Granger", "Weasley", "Longbottom", "Lovegood", "Malfoy", "Diggory",
    "Chang", "Finnigan", "Thomas", "Brown", "Patil", "Crabbe", "Goyle", "Parkinson",
    "Macmillan", "Abbott", "Finch-Fletchley", "Creevey", "Vane", "McLaggen", "Smith",
    "Flint", "Wood", "Bell", "Johnson", "Jordan", "Bulstrode", "Nott", "Zabini",
    "Bones", "Boot", "Corner", "Goldstein", "Myrtle",
]

PROFESSORS = {
    "Severus Snape": (25, "Slytherin", "Gryffindor"),
    "Minerva McGonagall": (20, "Gryffindor", None),
    "Albus Dumbledore": (5, "Gryffindor", None),
    "Filius Flitwick": (12, "Ravenclaw", None),
    "Pomona Sprout": (12, "Hufflepuff", None),
    "Gilderoy Lockhart": (6, None, None),
    "Dolores Umbridge": (8, "Slytherin", "Gryffindor"),
    "Rubeus Hagrid": (6, "Gryffindor", None),
    "Sybill Trelawney": (4, None, None),
    "Remus Lupin": (2, None, None),
}
PROF_NAMES = list(PROFESSORS)
PROF_WEIGHTS = [v[0] for v in PROFESSORS.values()]

REASONS_PLUS = [
    "Correctly pronounced Wingardium Leviosa",
    "Survived a Potions class without explosions",
    "Saved the school (again)",
    "Excellent essay on Hippogriff etiquette",
    "Found the Room of Requirement by accident",
    "Being Harry Potter",
    "Last-minute speech at the feast",
    "Not setting Seamus on fire",
    "Brewed a perfect Draught of Peace",
    "Predicted their own death correctly (for once)",
    "Laughed at Lockhart's joke convincingly",
    "Returned a library book on time",
]

REASONS_MINUS = [
    "Breathing too loudly near Snape",
    "Being a Gryffindor",
    "Out of bed after hours",
    "Exploded a cauldron",
    "Insulted Buckbeak",
    "Turned a teacup into a rat (it was meant to be the reverse)",
    "Fed Mandrakes after midnight",
    "Flew a car into the Whomping Willow",
    "Sold Weasley Wizard Wheezes in the corridor",
    "Hexed Peeves (Peeves started it)",
    "Lost a Remembrall",
    "Used a Howler as an alarm clock",
    "Called it Hogwarts Castle instead of Hogwarts",
    "Asked Trelawney about the weather",
    "Said Voldemort's name out loud",
]

LOCATIONS = [
    "Great Hall", "Potions Dungeon", "Gryffindor Common Room", "Slytherin Dungeon",
    "Astronomy Tower", "Forbidden Forest", "Quidditch Pitch", "Library Restricted Section",
    "Hagrid's Hut", "Moaning Myrtle's Bathroom", "Room of Requirement", "Owlery",
    "Greenhouse 3", "Kitchens", "Hogsmeade (unauthorized)", "Whomping Willow",
]

SPELLS = [
    "Expelliarmus", "Wingardium Leviosa", "Alohomora", "Lumos", "Nox", "Accio",
    "Stupefy", "Petrificus Totalus", "Riddikulus", "Expecto Patronum",
    "Obliviate", "Tarantallegra", "Rictusempra", "Furnunculus", "Reparo", "none",
]

WAND_WOODS = ["Holly", "Yew", "Willow", "Vine", "Hawthorn", "Elder", "Ash", "Cherry", "Oak", "Walnut"]
WAND_CORES = ["Phoenix feather", "Dragon heartstring", "Unicorn hair", "Thestral tail hair", "Veela hair"]

CREATURES = [
    "none", "none", "none", "none",
    "Hippogriff", "Cornish Pixie", "Niffler", "Blast-Ended Skrewt", "Mandrake",
    "Troll", "Acromantula", "Hungarian Horntail", "House-elf", "Crookshanks",
    "Mrs Norris", "Scabbers", "Fluffy", "Basilisk",
]


def points_for(rng: random.Random, professor: str, house: str) -> int:
    _, favorite, hated = PROFESSORS[professor]
    if professor == "Albus Dumbledore":
        return rng.choice([10, 50, 60, 100, 170])
    if house == hated:
        return -rng.randint(5, 50)
    if house == favorite:
        return rng.randint(1, 30)
    if professor == "Gilderoy Lockhart":
        return rng.choice([10, 10, 10, 5])
    if professor == "Sybill Trelawney":
        return rng.choice([-13, 13])
    return rng.randint(-10, 10) or 1


def generate_batch(rng: random.Random, start_id: int, n: int) -> list:
    profs = rng.choices(PROF_NAMES, weights=PROF_WEIGHTS, k=n)
    rows = []
    for k in range(n):
        incident_id = start_id + k
        day = incident_id % DAYS
        ts = START_DATE + timedelta(days=day, seconds=rng.randrange(86_400))

        professor = profs[k]
        house = rng.choice(HOUSES)
        if professor == "Albus Dumbledore" and rng.random() < 0.9:
            house = "Gryffindor"
        points = points_for(rng, professor, house)

        spell = rng.choice(SPELLS)
        creature = rng.choice(CREATURES)
        peeves = rng.random() < 0.15
        damage = 0.0
        if creature != "none" or peeves or points < 0:
            damage = round(rng.expovariate(1 / 40), 2)
        detention = rng.choice([0, 0, 0, 1, 2, 3]) if points < 0 else 0
        filch_anger = min(10, rng.randint(6, 9) + (1 if peeves else 0) + (1 if damage > 100 else 0))
        owl = points < -30 or rng.random() < 0.02
        howler = owl and rng.random() < 0.3

        rows.append((
            incident_id,
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            ts.strftime("%Y-%m-%d"),
            f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            house,
            rng.randint(1, 7),
            professor,
            points,
            rng.choice(REASONS_PLUS if points > 0 else REASONS_MINUS),
            rng.choice(LOCATIONS),
            spell,
            "false" if spell == "none" else ("true" if rng.random() < 0.7 else "false"),
            rng.choice(WAND_WOODS),
            rng.choice(WAND_CORES),
            creature,
            f"{damage:.2f}",
            detention,
            rng.randint(0, 25),
            "true" if peeves else "false",
            filch_anger,
            "true" if owl else "false",
            "true" if howler else "false",
        ))
    return rows


def main():
    parser = argparse.ArgumentParser(description="Generate Hogwarts house points ledger CSV")
    parser.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "events.csv"))
    parser.add_argument("--size-mb", type=int, default=1100, help="target file size in MB")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    target = args.size_mb * 1024 * 1024
    rng = random.Random(args.seed)

    written = 0
    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        while f.tell() < target:
            writer.writerows(generate_batch(rng, written, BATCH))
            written += BATCH
            print(f"\r{written:>12,} rows, {f.tell() / 1024 / 1024:8.1f} MB", end="", flush=True)
    print(f"\ndone: {args.out}")


if __name__ == "__main__":
    main()
