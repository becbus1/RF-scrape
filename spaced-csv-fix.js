#!/usr/bin/env node

/**
 * SPACED CSV FIX - Handles CSV with empty columns between data
 * 
 * Your CSV format: ZIP,,, BLDGNO1,,, STREET1,,, STSUFX1
 * Column positions: A, D, H, M (with empty columns in between)
 */

require('dotenv').config();
const fs = require('fs').promises;
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class SpacedCSVFix {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    async fixSpacedCSV() {
        console.log('🔧 SPACED CSV FIX - Handling CSV with Empty Columns');
        console.log('=' .repeat(60));

        try {
            // Read the Manhattan CSV
            const csvPath = 'data/dhcr/2023-DHCR-Bldg-File-Manhattan.csv';
            console.log(`📁 Reading: ${csvPath}`);
            
            const csvContent = await fs.readFile(csvPath, 'utf8');
            console.log(`📊 File size: ${Math.round(csvContent.length / 1024)}KB`);

            // Parse CSV without headers (treat as data array)
            const parsed = Papa.parse(csvContent, {
                header: false,
                skipEmptyLines: true,
                delimiter: ','
            });

            console.log(`📋 Total rows: ${parsed.data.length}`);

            // Show structure of first few rows
            console.log('\n📝 CSV Structure Analysis:');
            parsed.data.slice(0, 5).forEach((row, index) => {
                console.log(`Row ${index + 1}:`, row.slice(0, 15).map((cell, i) => `[${i}]${cell || 'empty'}`));
            });

            // Find header row (contains ZIP, BLDGNO1, etc.)
            let headerRowIndex = -1;
            let columnPositions = {};

            for (let i = 0; i < Math.min(5, parsed.data.length); i++) {
                const row = parsed.data[i];
                const positions = this.findColumnPositions(row);
                
                if (positions.zip >= 0 && positions.bldgno >= 0 && positions.street >= 0) {
                    headerRowIndex = i;
                    columnPositions = positions;
                    console.log(`\n✅ Found header structure at row ${i + 1}:`);
                    console.log(`   ZIP at column ${positions.zip}`);
                    console.log(`   BLDGNO1 at column ${positions.bldgno}`);
                    console.log(`   STREET1 at column ${positions.street}`);
                    console.log(`   STSUFX1 at column ${positions.suffix}`);
                    break;
                }
            }

            if (headerRowIndex === -1) {
                throw new Error('Could not find header row with expected columns');
            }

            // Process data rows
            const buildings = this.extractBuildingsWithPositions(
                parsed.data.slice(headerRowIndex + 1), 
                columnPositions
            );

            console.log(`✅ Extracted ${buildings.length} buildings`);

            // Show sample data
            if (buildings.length > 0) {
                console.log('\n📍 Sample buildings:');
                buildings.slice(0, 5).forEach((building, index) => {
                    console.log(`${index + 1}. ${building.address} (${building.borough}, ${building.zipcode})`);
                });
            }

            // Save to database
            const saved = await this.saveToDatabase(buildings);
            console.log(`🎉 SUCCESS: Saved ${saved} Manhattan buildings to Supabase!`);

            return saved;

        } catch (error) {
            console.error('❌ Fix failed:', error.message);
            throw error;
        }
    }

    /**
     * Find column positions by looking for header text
     */
    findColumnPositions(row) {
        const positions = { zip: -1, bldgno: -1, street: -1, suffix: -1 };
        
        for (let i = 0; i < row.length; i++) {
            const cell = (row[i] || '').toString().toUpperCase().trim();
            
            if (cell === 'ZIP' || cell === 'ZIPCODE') {
                positions.zip = i;
            } else if (cell === 'BLDGNO1' || cell === 'BUILDING_NUMBER' || cell === 'BLDG_NO') {
                positions.bldgno = i;
            } else if (cell === 'STREET1' || cell === 'STREET_NAME' || cell === 'STREET') {
                positions.street = i;
            } else if (cell === 'STSUFX1' || cell === 'STREET_SUFFIX' || cell === 'SUFFIX') {
                positions.suffix = i;
            }
        }
        
        return positions;
    }

    /**
     * Extract buildings using known column positions
     */
    extractBuildingsWithPositions(dataRows, positions) {
        const buildings = [];
        
        console.log(`🔄 Processing ${dataRows.length} data rows...`);
        
        for (const row of dataRows) {
            try {
                // Extract values from specific column positions
                const zip = row[positions.zip] || '';
                const bldgNo = row[positions.bldgno] || '';
                const street = row[positions.street] || '';
                const suffix = row[positions.suffix] || '';

                // Validate data looks reasonable
                if (!this.isValidBuildingData(zip, bldgNo, street)) {
                    continue;
                }

                // Create building record
                const building = this.createBuildingRecord(zip, bldgNo, street, suffix);
                if (building) {
                    buildings.push(building);
                }

            } catch (error) {
                continue; // Skip invalid rows
            }
        }

        console.log(`   ✅ Extracted ${buildings.length} valid buildings`);
        return this.deduplicateBuildings(buildings);
    }

    /**
     * Validate building data
     */
    isValidBuildingData(zip, bldgNo, street) {
        // Check ZIP (should be 5 digits, NYC starts with 10/11)
        const zipStr = zip.toString().replace(/[^0-9]/g, '');
        if (zipStr.length !== 5 || (!zipStr.startsWith('10') && !zipStr.startsWith('11'))) {
            return false;
        }

        // Check building number (should exist and be reasonable)
        const bldgStr = bldgNo.toString().trim();
        if (!bldgStr || bldgStr.length === 0 || bldgStr.length > 10) {
            return false;
        }

        // Check street name (should exist)
        const streetStr = street.toString().trim();
        if (!streetStr || streetStr.length < 2) {
            return false;
        }

        return true;
    }

    /**
     * Create building record
     */
    createBuildingRecord(zip, bldgNo, street, suffix) {
        // Build address from components
        let address = '';
        if (bldgNo) address += bldgNo.toString().trim();
        if (street) address += ' ' + street.toString().trim();
        if (suffix && suffix.toString().trim()) address += ' ' + suffix.toString().trim();
        
        address = address.trim().toUpperCase();

        // Validate final address
        if (!address || address.length < 5) {
            return null;
        }

        return {
            address: address,
            normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
            borough: 'manhattan',
            zipcode: zip.toString().replace(/[^0-9]/g, '').substring(0, 5),
            building_id: null,
            unit_count: null,
            registration_id: null,
            dhcr_source: 'csv',
            confidence_score: 95,
            verification_status: 'unverified',
            parsed_at: new Date().toISOString(),
            created_at: new Date().toISOString()
        };
    }

    /**
     * Remove duplicates
     */
    deduplicateBuildings(buildings) {
        const seen = new Set();
        const unique = [];
        
        for (const building of buildings) {
            const key = `${building.normalized_address}-${building.borough}`;
            
            if (!seen.has(key)) {
                seen.add(key);
                unique.push(building);
            }
        }
        
        console.log(`🔄 Deduplicated: ${buildings.length} → ${unique.length} buildings`);
        return unique;
    }

    /**
     * Save to database
     */
    async saveToDatabase(buildings) {
        if (buildings.length === 0) {
            console.log('📊 No buildings to save');
            return 0;
        }
        
        try {
            console.log(`💾 Saving ${buildings.length} buildings to Supabase...`);
            
            const batchSize = 500;
            let saved = 0;
            
            for (let i = 0; i < buildings.length; i += batchSize) {
                const batch = buildings.slice(i, i + batchSize);
                
                const { error } = await this.supabase
                    .from('rent_stabilized_buildings')
                    .upsert(batch, { 
                        onConflict: 'normalized_address,borough'
                    });
                
                if (error) {
                    console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
                    continue;
                }
                
                saved += batch.length;
                console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}: ${saved}/${buildings.length} saved`);
            }
            
            console.log(`🎉 Total saved: ${saved} buildings`);
            return saved;
            
        } catch (error) {
            console.error('❌ Database save failed:', error.message);
            throw error;
        }
    }
}

// Main execution
async function main() {
    const fixer = new SpacedCSVFix();
    
    try {
        const saved = await fixer.fixSpacedCSV();
        
        if (saved > 0) {
            console.log('\n🚀 NEXT STEPS:');
            console.log('1. Deploy your Railway app again');
            console.log('2. It should now find rent-stabilized buildings!');
            console.log('3. Test with: TEST_NEIGHBORHOOD=soho');
        } else {
            console.log('\n❌ No buildings were saved. Check the CSV structure analysis above.');
        }
        
    } catch (error) {
        console.error('💥 Spaced CSV fix failed:', error.message);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main();
}
