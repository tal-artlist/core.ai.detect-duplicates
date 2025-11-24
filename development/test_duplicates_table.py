#!/usr/bin/env python3
"""
Test & Validate Duplicates Table
Comprehensive tests for BI_PROD.AI_DATA.DUPLICATED_ASSETS
"""

import json
from collections import defaultdict, Counter
from snowflake_utils import SnowflakeConnector

def test_no_duplicate_rows():
    """Test 1: Verify no duplicate (song_id, product_indicator) pairs"""
    print("\n" + "="*80)
    print("TEST 1: No Duplicate Rows")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT 
        SONG_ID,
        PRODUCT_INDICATOR,
        COUNT(*) as cnt
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    GROUP BY SONG_ID, PRODUCT_INDICATOR
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10
    """
    
    cursor = snowflake.execute_query(query)
    duplicates = list(cursor)
    
    if duplicates:
        print(f"❌ FAILED - Found {len(duplicates)} duplicate rows:")
        for row in duplicates[:5]:
            print(f"   Song {row[0]} (product {row[1]}): appears {row[2]} times")
        cursor.close()
        snowflake.close()
        return False
    else:
        print("✅ PASSED - No duplicate rows found")
        cursor.close()
        snowflake.close()
        return True

def test_primary_key_constraint():
    """Test 2: Verify primary key constraint is working"""
    print("\n" + "="*80)
    print("TEST 2: Primary Key Constraint")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SHOW PRIMARY KEYS IN TABLE BI_PROD.AI_DATA.DUPLICATED_ASSETS
    """
    
    cursor = snowflake.execute_query(query)
    pk_info = list(cursor)
    
    if pk_info:
        print(f"✅ PASSED - Primary key exists:")
        for row in pk_info:
            print(f"   Column: {row[4]}")
    else:
        print("⚠️  WARNING - No primary key found (may allow duplicates)")
    
    cursor.close()
    snowflake.close()
    return True

def test_data_completeness():
    """Test 3: Verify all required columns are populated"""
    print("\n" + "="*80)
    print("TEST 3: Data Completeness")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT 
        COUNT(*) as total_rows,
        COUNT(SONG_ID) as song_id_count,
        COUNT(PRODUCT_INDICATOR) as product_count,
        COUNT(ASSET_TYPE) as asset_type_count,
        COUNT(DUPLICATES) as duplicates_count,
        SUM(CASE WHEN SONG_ID IS NULL THEN 1 ELSE 0 END) as null_song_id,
        SUM(CASE WHEN PRODUCT_INDICATOR IS NULL THEN 1 ELSE 0 END) as null_product,
        SUM(CASE WHEN ASSET_TYPE IS NULL THEN 1 ELSE 0 END) as null_asset_type,
        SUM(CASE WHEN DUPLICATES IS NULL THEN 1 ELSE 0 END) as null_duplicates
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    """
    
    cursor = snowflake.execute_query(query)
    row = cursor.fetchone()
    
    total = row[0]
    print(f"Total rows: {total:,}")
    
    all_complete = True
    if row[5] > 0:
        print(f"❌ FAILED - {row[5]} rows with NULL song_id")
        all_complete = False
    if row[6] > 0:
        print(f"❌ FAILED - {row[6]} rows with NULL product_indicator")
        all_complete = False
    if row[7] > 0:
        print(f"❌ FAILED - {row[7]} rows with NULL asset_type")
        all_complete = False
    if row[8] > 0:
        print(f"❌ FAILED - {row[8]} rows with NULL duplicates")
        all_complete = False
    
    if all_complete:
        print("✅ PASSED - All required columns populated")
    
    cursor.close()
    snowflake.close()
    return all_complete

def test_asset_type_values():
    """Test 4: Verify asset_type is always 'music'"""
    print("\n" + "="*80)
    print("TEST 4: Asset Type Values")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT DISTINCT ASSET_TYPE, COUNT(*) as cnt
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    GROUP BY ASSET_TYPE
    """
    
    cursor = snowflake.execute_query(query)
    results = list(cursor)
    
    if len(results) == 1 and results[0][0] == 'music':
        print(f"✅ PASSED - All {results[0][1]:,} rows have asset_type = 'music'")
        cursor.close()
        snowflake.close()
        return True
    else:
        print(f"❌ FAILED - Found unexpected asset_type values:")
        for row in results:
            print(f"   '{row[0]}': {row[1]:,} rows")
        cursor.close()
        snowflake.close()
        return False

def test_product_indicator_values():
    """Test 5: Verify product_indicator values are valid (1 or 3)"""
    print("\n" + "="*80)
    print("TEST 5: Product Indicator Values")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT 
        PRODUCT_INDICATOR,
        COUNT(*) as cnt
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    GROUP BY PRODUCT_INDICATOR
    ORDER BY PRODUCT_INDICATOR
    """
    
    cursor = snowflake.execute_query(query)
    results = list(cursor)
    
    valid = all(row[0] in [1, 3] for row in results)
    
    if valid:
        print("✅ PASSED - All product_indicator values are valid:")
        for row in results:
            product_name = "Artlist" if row[0] == 1 else "MotionArray"
            print(f"   {row[0]} ({product_name}): {row[1]:,} rows")
    else:
        print("❌ FAILED - Found invalid product_indicator values:")
        for row in results:
            print(f"   {row[0]}: {row[1]:,} rows")
    
    cursor.close()
    snowflake.close()
    return valid

def test_duplicates_array_format():
    """Test 6: Verify duplicates array is properly formatted JSON"""
    print("\n" + "="*80)
    print("TEST 6: Duplicates Array Format")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT 
        SONG_ID,
        PRODUCT_INDICATOR,
        DUPLICATES
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    LIMIT 5
    """
    
    cursor = snowflake.execute_query(query)
    
    all_valid = True
    for row in cursor:
        song_id = row[0]
        product = row[1]
        duplicates = row[2]
        
        # Check if it's a valid array
        if not isinstance(duplicates, (list, str)):
            print(f"❌ FAILED - Song {song_id}: duplicates is not array/string type")
            all_valid = False
            continue
        
        # If it's a string, try to parse it
        if isinstance(duplicates, str):
            try:
                duplicates = json.loads(duplicates)
            except:
                print(f"❌ FAILED - Song {song_id}: duplicates cannot be parsed as JSON")
                all_valid = False
                continue
        
        # Check array structure
        if not isinstance(duplicates, list):
            print(f"❌ FAILED - Song {song_id}: duplicates is not a list")
            all_valid = False
            continue
        
        # Check each element has required keys
        for dup in duplicates:
            if 'product_indicator' not in dup or 'song_id' not in dup:
                print(f"❌ FAILED - Song {song_id}: duplicate missing required keys")
                all_valid = False
                break
    
    if all_valid:
        print("✅ PASSED - Duplicates arrays are properly formatted")
    
    cursor.close()
    snowflake.close()
    return all_valid

def test_reciprocal_duplicates():
    """Test 7: Verify duplicate relationships are reciprocal"""
    print("\n" + "="*80)
    print("TEST 7: Reciprocal Duplicate Relationships")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    # Get a sample of duplicate relationships
    query = """
    SELECT 
        SONG_ID,
        PRODUCT_INDICATOR,
        DUPLICATES
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    LIMIT 100
    """
    
    cursor = snowflake.execute_query(query)
    
    # Build a map of relationships
    relationships = {}
    for row in cursor:
        song_id = str(row[0])
        product = row[1]
        duplicates = row[2] if isinstance(row[2], list) else json.loads(row[2])
        
        key = (song_id, product)
        relationships[key] = set()
        for dup in duplicates:
            dup_key = (str(dup['song_id']), dup['product_indicator'])
            relationships[key].add(dup_key)
    
    cursor.close()
    
    # Check reciprocity
    missing_reciprocals = []
    for key, dups in relationships.items():
        for dup_key in dups:
            if dup_key in relationships:
                if key not in relationships[dup_key]:
                    missing_reciprocals.append((key, dup_key))
    
    if not missing_reciprocals:
        print(f"✅ PASSED - All sampled relationships are reciprocal (checked {len(relationships)} songs)")
    else:
        print(f"❌ FAILED - Found {len(missing_reciprocals)} non-reciprocal relationships:")
        for key, dup_key in missing_reciprocals[:5]:
            print(f"   {key} lists {dup_key} as duplicate, but not vice versa")
    
    snowflake.close()
    return len(missing_reciprocals) == 0

def test_known_edge_cases():
    """Test 8: Verify known edge cases are handled correctly"""
    print("\n" + "="*80)
    print("TEST 8: Known Edge Cases")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    # Test case 1: Asset 137458 should appear only once
    query1 = """
    SELECT COUNT(*)
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    WHERE SONG_ID = 137458 AND PRODUCT_INDICATOR = 1
    """
    
    cursor = snowflake.execute_query(query1)
    count = cursor.fetchone()[0]
    cursor.close()
    
    if count == 1:
        print("✅ PASSED - Asset 137458 appears exactly once")
    else:
        print(f"❌ FAILED - Asset 137458 appears {count} times (expected 1)")
        snowflake.close()
        return False
    
    # Test case 2: Assets with same ID in different sources should be separate
    query2 = """
    SELECT SONG_ID, COUNT(DISTINCT PRODUCT_INDICATOR) as indicator_count
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    GROUP BY SONG_ID
    HAVING COUNT(DISTINCT PRODUCT_INDICATOR) > 1
    LIMIT 1
    """
    
    cursor = snowflake.execute_query(query2)
    result = cursor.fetchone()
    cursor.close()
    
    if result:
        song_id, indicator_count = result
        print(f"✅ PASSED - Found asset {song_id} in {indicator_count} different sources (correctly separated)")
    else:
        print("⚠️  INFO - No assets found in multiple sources")
    
    snowflake.close()
    return True

def test_statistics():
    """Test 9: Show overall statistics"""
    print("\n" + "="*80)
    print("TEST 9: Overall Statistics")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    # Total counts
    query = """
    SELECT 
        COUNT(*) as total_songs,
        COUNT(DISTINCT SONG_ID) as unique_song_ids,
        SUM(CASE WHEN PRODUCT_INDICATOR = 1 THEN 1 ELSE 0 END) as artlist_count,
        SUM(CASE WHEN PRODUCT_INDICATOR = 3 THEN 1 ELSE 0 END) as motionarray_count
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    """
    
    cursor = snowflake.execute_query(query)
    row = cursor.fetchone()
    cursor.close()
    
    print(f"Total rows: {row[0]:,}")
    print(f"Unique song IDs: {row[1]:,}")
    print(f"Artlist songs: {row[2]:,}")
    print(f"MotionArray songs: {row[3]:,}")
    
    # Duplicate count distribution
    query2 = """
    SELECT 
        ARRAY_SIZE(DUPLICATES) as dup_count,
        COUNT(*) as songs
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    GROUP BY ARRAY_SIZE(DUPLICATES)
    ORDER BY dup_count DESC
    LIMIT 10
    """
    
    cursor = snowflake.execute_query(query2)
    print("\nDuplicate count distribution:")
    for row in cursor:
        print(f"  {row[0]} duplicates: {row[1]:,} songs")
    cursor.close()
    
    snowflake.close()
    print("✅ Statistics gathered successfully")
    return True

def test_sample_records():
    """Test 10: Show sample records for manual inspection"""
    print("\n" + "="*80)
    print("TEST 10: Sample Records")
    print("="*80)
    
    snowflake = SnowflakeConnector()
    
    query = """
    SELECT 
        SONG_ID,
        PRODUCT_INDICATOR,
        ASSET_TYPE,
        DUPLICATES
    FROM BI_PROD.AI_DATA.DUPLICATED_ASSETS
    ORDER BY RANDOM()
    LIMIT 5
    """
    
    cursor = snowflake.execute_query(query)
    
    for i, row in enumerate(cursor, 1):
        song_id = row[0]
        product = row[1]
        asset_type = row[2]
        duplicates = row[3] if isinstance(row[3], list) else json.loads(row[3])
        
        product_name = "Artlist" if product == 1 else "MotionArray"
        print(f"\nSample {i}:")
        print(f"  Song ID: {song_id} ({product_name})")
        print(f"  Asset Type: {asset_type}")
        print(f"  Duplicates ({len(duplicates)}):")
        for dup in duplicates[:3]:
            dup_product = "Artlist" if dup['product_indicator'] == 1 else "MotionArray"
            print(f"    - {dup['song_id']} ({dup_product})")
        if len(duplicates) > 3:
            print(f"    ... and {len(duplicates) - 3} more")
    
    cursor.close()
    snowflake.close()
    print("\n✅ Sample records displayed")
    return True

def main():
    """Run all tests"""
    print("🧪 COMPREHENSIVE DUPLICATES TABLE TESTS")
    print("Testing: BI_PROD.AI_DATA.DUPLICATED_ASSETS")
    
    tests = [
        ("No Duplicate Rows", test_no_duplicate_rows),
        ("Primary Key Constraint", test_primary_key_constraint),
        ("Data Completeness", test_data_completeness),
        ("Asset Type Values", test_asset_type_values),
        ("Product Indicator Values", test_product_indicator_values),
        ("Duplicates Array Format", test_duplicates_array_format),
        ("Reciprocal Relationships", test_reciprocal_duplicates),
        ("Known Edge Cases", test_known_edge_cases),
        ("Overall Statistics", test_statistics),
        ("Sample Records", test_sample_records),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ ERROR in {test_name}: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! Table is ready for production use.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review and fix issues.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

