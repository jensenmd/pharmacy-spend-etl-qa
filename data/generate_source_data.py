"""
generate_source_data.py

Generates synthetic CMS Medicare Part D-style pharmaceutical spend data.
Mimics the structure of real CMS Part D public datasets.

Run: python data/generate_source_data.py
Output: data/raw/part_d_spending_2022.csv
"""

import csv
import random
import os

random.seed(42)  # Reproducible data generation

MANUFACTURERS = [
    "AstraZeneca", "Pfizer", "Johnson & Johnson", "Merck", "Novartis",
    "Roche", "Abbott", "Eli Lilly", "Bristol-Myers Squibb", "GlaxoSmithKline",
    "Amgen", "Gilead Sciences", "Biogen", "Regeneron", "Moderna"
]

DRUG_CATEGORIES = [
    "Cardiovascular", "Diabetes", "Oncology", "Immunology",
    "Neurology", "Respiratory", "Infectious Disease", "Gastroenterology"
]

DRUGS = [
    ("ELIQUIS", "APIXABAN", "Bristol-Myers Squibb", "Cardiovascular"),
    ("XARELTO", "RIVAROXABAN", "Johnson & Johnson", "Cardiovascular"),
    ("JARDIANCE", "EMPAGLIFLOZIN", "Eli Lilly", "Diabetes"),
    ("OZEMPIC", "SEMAGLUTIDE", "Novo Nordisk", "Diabetes"),
    ("TRULICITY", "DULAGLUTIDE", "Eli Lilly", "Diabetes"),
    ("KEYTRUDA", "PEMBROLIZUMAB", "Merck", "Oncology"),
    ("REVLIMID", "LENALIDOMIDE", "Bristol-Myers Squibb", "Oncology"),
    ("HUMIRA", "ADALIMUMAB", "AbbVie", "Immunology"),
    ("DUPIXENT", "DUPILUMAB", "Regeneron", "Immunology"),
    ("TECFIDERA", "DIMETHYL FUMARATE", "Biogen", "Neurology"),
    ("SPIRIVA", "TIOTROPIUM BROMIDE", "Boehringer Ingelheim", "Respiratory"),
    ("ADVAIR DISKUS", "FLUTICASONE/SALMETEROL", "GlaxoSmithKline", "Respiratory"),
    ("HARVONI", "LEDIPASVIR/SOFOSBUVIR", "Gilead Sciences", "Infectious Disease"),
    ("BIKTARVY", "BICTEGRAVIR/FTC/TAF", "Gilead Sciences", "Infectious Disease"),
    ("NEXIUM", "ESOMEPRAZOLE MAGNESIUM", "AstraZeneca", "Gastroenterology"),
    ("XELJANZ", "TOFACITINIB", "Pfizer", "Immunology"),
    ("ENBREL", "ETANERCEPT", "Amgen", "Immunology"),
    ("LANTUS", "INSULIN GLARGINE", "Sanofi", "Diabetes"),
    ("JANUVIA", "SITAGLIPTIN", "Merck", "Diabetes"),
    ("LIPITOR", "ATORVASTATIN CALCIUM", "Pfizer", "Cardiovascular"),
    ("CRESTOR", "ROSUVASTATIN CALCIUM", "AstraZeneca", "Cardiovascular"),
    ("PLAVIX", "CLOPIDOGREL BISULFATE", "Bristol-Myers Squibb", "Cardiovascular"),
    ("AVASTIN", "BEVACIZUMAB", "Roche", "Oncology"),
    ("HERCEPTIN", "TRASTUZUMAB", "Roche", "Oncology"),
    ("LYRICA", "PREGABALIN", "Pfizer", "Neurology"),
]

def generate_part_d_data(year=2022, num_records=500):
    records = []

    for drug_name, generic_name, manufacturer, category in DRUGS:
        # Generate 15-25 records per drug (different dosages/forms)
        num_variants = random.randint(15, 25)
        for _ in range(num_variants):
            claim_count = random.randint(1000, 500000)
            spending_per_claim = round(random.uniform(50, 8000), 2)
            total_spending = round(claim_count * spending_per_claim, 2)
            beneficiary_count = int(claim_count * random.uniform(0.7, 0.95))
            unit_count = claim_count * random.randint(30, 90)

            records.append({
                "drug_name": drug_name,
                "generic_name": generic_name,
                "manufacturer": manufacturer,
                "drug_category": category,
                "claim_count": claim_count,
                "total_spending": total_spending,
                "spending_per_claim": spending_per_claim,
                "beneficiary_count": beneficiary_count,
                "unit_count": unit_count,
                "year": year
            })

    # Shuffle for realism
    random.shuffle(records)
    return records


def write_csv(records, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [
        "drug_name", "generic_name", "manufacturer", "drug_category",
        "claim_count", "total_spending", "spending_per_claim",
        "beneficiary_count", "unit_count", "year"
    ]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"Generated {len(records)} records -> {output_path}")


if __name__ == "__main__":
    records = generate_part_d_data(year=2022)
    write_csv(records, "data/raw/part_d_spending_2022.csv")
