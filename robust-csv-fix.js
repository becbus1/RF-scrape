#!/usr/bin/env node

/**
 * ROBUST CSV FIX - Handles malformed headers
 * 
 * This version fixes CSV parsing issues and works with bad headers
 */

require('dotenv').config();
const fs = require('fs').promises;
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class RobustCSVFix {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    async fixManhattanCSV() {
        console.log('🔧 ROBUST FIX: Manhattan DHCR CSV → Supabase');
        console.log('=' .repeat(50));

        try {
            // Read the Manhattan CSV
            const csvPath = 'data/dhcr/2023-DHCR-Bldg-File-Manhattan.csv';
            console.log(`📁 Reading: ${csvPath}`);
            
            const csvContent = await fs.readFile(csvPath, 'utf8');
            console.log(`📊 File size: ${Math.round(csvContent.length / 1024)}KB`);

            // Show first few lines to debug
            const lines = csvContent.split('\n').slice(0, 5);
            console.log('\n📝 First 5 lines of CSV:');
            lines.forEach((line, index) => {
                console.log(`${index + 1}: ${line.substring(0, 100)}...`);
            });

            // Try different parsing approaches
            let parsed = null;
            let approach = '';

            // Approach 1: Parse without headers (treat first row as data)
            console.log('\n🔄 Trying approach 1: No headers...');
            const noHeaderParsed = Papa.parse(csvContent, {
                header: false,
                skipEmptyLines: true,
                delimiter: ','
            });

            if (noHeaderParsed.data && noHeaderParsed.data.length > 0) {
                console.log(`✅ No-header approach: ${noHeaderParsed.data.length} rows`);
                console.log(`📝 First row sample:`, noHeaderParsed.data[0]?.slice(0, 10));
                
                // Use this approach if we have data
                parsed = this.convertNoHeaderData(noHeaderParsed.data);
                approach = 'no-header';
            }

            // Approach 2: Try different delimiters
            if (!parsed || parsed.length === 0) {
                console.log('\n🔄 Trying approach 2: Tab delimiter...');
                const tabParsed = Papa.parse(csvContent, {
                    header: false,
                    skipEmptyLines: true,
                    delimiter: '\t'
                });

                if (tabParsed.data && tabParsed.data.length > 0) {
                    console.log(`✅ Tab approach: ${tabParsed.data.length} rows`);
                    parsed = this.convertNoHeaderData(tabParsed.data);
                    approach = 'tab-delimited';
                }
            }

            // Approach 3: Manual line parsing
            if (!parsed || parsed.length === 0) {
                console.log('\n🔄 Trying approach 3: Manual line parsing...');
                parsed = this.parseManually(csvContent);
                approach = 'manual';
            }

            if (!parsed || parsed.length === 0) {
                throw new Error('All parsing approaches failed');
            }

            console.log(`✅ Success with ${approach} approach: ${parsed.length} buildings`);

            // Show sample data
            if (parsed.length > 0) {
                console.log('\n📍 Sample buildings:');
                parsed.slice(0, 5).forEach((building, index) => {
                    console.log(`${index + 1}. ${building.address} (${building.borough}, ${building.zipcode})`);
                });
            }

            // Save to database
            const saved = await this.saveToDatabase(parsed);
            console.log(`🎉 SUCCESS: Saved ${saved} Manhattan buildings to Supabase!`);

            return saved;

        } catch (error) {
            console.error('❌ Fix failed:', error.message);
            throw error;
        }
    }

    /**
     * Convert data without headers (assuming column positions)
     */
    convertNoHeaderData(rows) {
        const buildings = [];
        
        // Skip first row if it looks like headers
        let startIndex = 0;
        if (rows.length > 0) {
            const firstRow = rows[0];
            if (firstRow.some(cell => cell && cell.toString().toUpperCase().includes('ZIP'))) {
                startIndex = 1;
                console.log('   📋 Detected header row, skipping...');
            }
        }

        for (let i = startIndex; i < rows.length; i++) {
            const row = rows[i];
            
            if (!row || row.length < 4) continue;

            try {
                // Assume DHCR format: [ZIP, BLDGNO1, STREET1, STSUFX1, ...]
                // Try different column positions
                const building = this.extractBuildingFromRow(row);
                
                if (building) {
                    buildings.push(building);
                }
                
            } catch (error) {
                continue; // Skip invalid rows
            }
        }

        console.log(`   ✅ Extracted ${buildings.length} buildings from ${rows.length} rows`);
        return this.deduplicateBuildings(buildings);
    }

    /**
     * Extract building from row (try different column arrangements)
     */
    extractBuildingFromRow(row) {
        // Try different possible arrangements
        const arrangements = [
            // [zipIndex, bldgIndex, streetIndex, suffixIndex]
            [0, 1, 2, 3],  // ZIP, BLDGNO1, STREET1, STSUFX1
            [1, 2, 3, 4],  // Skip first column
            [0, 2, 3, 4],  // Different arrangement
            [1, 0, 2, 3],  // BLDGNO1 first
        ];

        for (const [zipIdx, bldgIdx, streetIdx, suffixIdx] of arrangements) {
            try {
                const zip = row[zipIdx] || '';
                const bldgNo = row[bldgIdx] || '';
                const street = row[streetIdx] || '';
                const suffix = row[suffixIdx] || '';

                // Validate this looks like building data
                if (this.looksLikeBuildingData(zip, bldgNo, street)) {
                    return this.createBuildingRecord(zip, bldgNo, street, suffix);
                }
            } catch (error) {
                continue;
            }
        }

        return null;
    }

    /**
     * Check if data looks like building information
     */
    looksLikeBuildingData(zip, bldgNo, street) {
        // ZIP should be 5 digits (or close)
        const zipNum = zip.toString().replace(/[^0-9]/g, '');
        if (zipNum.length < 4 || zipNum.length > 5) return false;

        // Building number should exist and be reasonable
        const bldgNum = bldgNo.toString().replace(/[^0-9]/g, '');
        if (!bldgNum || bldgNum.length === 0 || bldgNum.length > 6) return false;

        // Street should exist and look like a street name
        const streetStr = street.toString().trim();
        if (!streetStr || streetStr.length < 2) return false;

        // NYC ZIP codes start with 10, 11 (common check)
        if (!zipNum.startsWith('10') && !zipNum.startsWith('11')) return false;

        return true;
    }

    /**
     * Create building record
     */
    createBuildingRecord(zip, bldgNo, street, suffix) {
        // Build address
        let address = '';
        if (bldgNo) address += bldgNo.toString().trim();
        if (street) address += ' ' + street.toString().trim();
        if (suffix) address += ' ' + suffix.toString().trim();
        
        address = address.trim().toUpperCase();

        // Validate
        if (!address || address.length < 5) return null;

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
     * Manual parsing as last resort
     */
    parseManually(csvContent) {
        console.log('   🔧 Manual parsing...');
        
        const lines = csvContent.split('\n');
        const buildings = [];
        
        for (let i = 1; i < lines.length; i++) { // Skip first line
            const line = lines[i].trim();
            if (!line) continue;

            // Split by comma, but handle quoted fields
            const fields = line.split(',').map(f => f.trim().replace(/"/g, ''));
            
            if (fields.length >= 4) {
                // Try to extract building data
                for (let start = 0; start <= Math.min(2, fields.length - 4); start++) {
                    const zip = fields[start];
                    const bldgNo = fields[start + 1];
                    const street = fields[start + 2];
                    const suffix = fields[start + 3];
                    
                    if (this.looksLikeBuildingData(zip, bldgNo, street)) {
                        const building = this.createBuildingRecord(zip, bldgNo, street, suffix);
                        if (building) {
                            buildings.push(building);
                            break; // Found valid building in this line
                        }
                    }
                }
            }
        }

        console.log(`   ✅ Manual parsing extracted ${buildings.length} buildings`);
        return this.deduplicateBuildings(buildings);
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
    const fixer = new RobustCSVFix();
    
    try {
        const saved = await fixer.fixManhattanCSV();
        
        if (saved > 0) {
            console.log('\n🚀 NEXT STEPS:');
            console.log('1. Deploy your Railway app again');
            console.log('2. It should now find rent-stabilized buildings!');
            console.log('3. Test with: TEST_NEIGHBORHOOD=soho');
        } else {
            console.log('\n❌ No buildings were saved. CSV format may need manual fixing.');
            console.log('💡 Check the sample lines above to debug the format.');
        }
        
    } catch (error) {
        console.error('💥 Robust fix failed:', error.message);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main();
}
