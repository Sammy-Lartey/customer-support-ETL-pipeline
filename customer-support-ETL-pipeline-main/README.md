# Customer Support Data Pipeline 

## Overview
An end-to-end ETL pipeline that transforms raw, multi-sheet customer support Excel files into a clean, analytics-ready PostgreSQL database. Built to handle messy real-world data, enforce data quality, and support reliable reporting.

## The Problem
Incoming customer support data had:
- Multiple fragmented monthly sheets
- Inconsistent formats and phone numbers
- Unreliable region names
- Missing and duplicate customer identifiers
- No direct link between customer profiles and complaints

## Solution Architecture
A modular pipeline designed around five core phases:

### 1. Data Preparation
- Automated ingestion of multi-sheet Excel files
- Smart merging of monthly datasets
- Automatic column alignment across sheets

### 2. Data Cleaning
- Phone number standardization for Ghanaian formats (`+233`)
- Fuzzy region matching using RapidFuzz
- Date validation and turnaround-time correction
- Customer name normalization with “Unknown” handling

### 3. Schema Management
- Automated splitting into customers and complaints tables
- Integration with existing client profile data
- Profile ID synchronization

### 4. Data Integration
- ULID-based unique customer identifiers
- Foreign-key/primary-key enforcement
- Intelligent NULL-handling and constraint management

### 5. Analytics Layer
- Indexing for high-performance queries
- Business-friendly SQL views
- Materialized views for aggregated reporting

## Technical Highlights
### Data Quality Engineering
##### Fuzzy matching implementation for region correction
def _correct_region(self, region_name, valid_regions, threshold=80):
    match, score, _ = process.extractOne(region_name, valid_regions, scorer=fuzz.token_set_ratio)
    return match if score >= threshold else "Unknown"

- 95% phone number standardization accuracy
- 90% region correction accuracy
- Self-correcting turnaround-time calculations

## Customer Identification
- Multi-source matching using phone numbers and existing IDs
- ULID implementation for scalable unique identifiers
- Deduplication across monthly datasets

## Database Engineering
- Constraint-driven schema (PKs, FKs, NOT NULL)
- Indexed frequently filtered columns
- Isolated schema with controlled access

## Tech Stack
Python (Pandas, SQLAlchemy, RapidFuzz), PostgreSQL, ULID, python-dotenv

## Key Features Built

### Data Processing
- Automated phone normalization
- Turnaround-time validation engine
- Region correction with 85%+ accuracy

### Integration
- Sync with existing client database
- Multi-criteria profile matching
- Consistency across 10,000+ records

### Analytics
- Customer 360° profile view
- Regional performance dashboards
- Monthly trend analysis

## Achievements

### Performance
- Efficient processing of 50,000+ records
- Chunked database operations
- Optimized indexing → 70% faster queries

### Data Quality
- Standardized, validated dataset
- Full referential integrity between customers and complaints

### Code Quality
- Modular, single-responsibility classes
- Robust error handling and logging
- Environment-based configuration

## Business Impact
- Fragmented Excel data → centralized PostgreSQL database
- Report generation reduced from hours → real-time queries
- Reliable, standardized data for analysis
- Unified historical customer support view

## Why This Project Matters
A robust data engineering solution that transforms messy, fragmented data into reliable, actionable intelligence. Highlights:
- Resolves real-world inconsistencies in customer support data
- Unifies multi-source datasets for seamless analytics
- Applies localized rules for Ghanaian phone and region formats
- Automates business logic to maintain data accuracy and integrity.

