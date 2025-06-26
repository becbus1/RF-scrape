#!/usr/bin/env node

/**
 * SIMPLE CSV SAVE - No conflict resolution, just insert
 */

require('dotenv').config();
const fs = require('fs').promises;
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class SimpleCSVSave {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    async saveSimple() {
        console.log('🔧 SIMPLE CSV SAVE - No Conflict Resolution');
        console.log('=' .repeat(50));

        try {
            // Clear existing data first
            console.log('🗑️ Clearing existing data...');
            const { error: deleteError } = await this.supabase
                .from('rent_stabilized_buildings')
                .delete()
                .neq('id', '00000000-0000-0000-0000-000000000000'); // Delete all

            if (deleteError) {
                console.warn('⚠️ Could not clear existing data:', deleteError.message);
            } else {
                console.log('✅ Cleared existing data');
            }

            // Read and parse CSV
            const csvPath = 'data/dhcr/2023-DHCR-Bldg-File-Manhattan.csv';
            const csvContent = await fs.readFile(csvPath, 'utf8');
            
            const parsed = Papa.parse(csvContent, {
                header: false,
                skipEmptyLines: true,
                delimiter: ','
            });

            // Find header row and extract buildings
            const buildings = this.extractBuildings(parsed.data);
            console.log(`✅ Extracted ${buildings.length} buildings`);

            // Save with simple insert (no conflict resolution)
            console.log('💾 Saving to Supabase (simple insert)...');
            
            const batchSize = 500;
            let saved = 0;
            
            for (let i = 0; i < buildings.length; i += batchSize) {
                const batch = buildings.slice(i, i + batchSize);
                
                const { error } = await this.supabase
                    .from('rent_stabilized_buildings')
                    .insert(batch); // Simple insert, no upsert
                
                if (error) {
                    console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
                    continue;
                }
                
                saved += batch.length;
                console.log(`✅ Batch ${Math.floor(i/batchSize) + 1}: ${saved}/${buildings.length} saved`);
            }
            
            console.log(`🎉 SUCCESS: Saved ${saved} buildings to Supabase!`);
            return saved;

        } catch (error) {
            console.error('❌ Save failed:', error.message);
            throw error;
        }
    }

    extractBuildings(rows) {
        const buildings = [];
        
        // Find header row
        let headerRowIndex = -1;
        for (let i = 0; i < Math.min(5, rows.length); i++) {
            const row = rows[i];
            if (row.some(cell => cell && cell.toString().toUpperCase().includes('ZIP'))) {
                headerRowIndex = i;
                break;
            }
        }

        if (headerRowIndex === -1) {
            console.warn('⚠️ No header row found, using row 2');
            headerRowIndex = 1;
        }

        // Process data rows
        for (let i = headerRowIndex + 1; i < rows.length; i++) {
            const row = rows[i];
            
            if (!row || row.length < 4) continue;

            try {
                // Extract using known positions: ZIP(0), BLDGNO1(3), STREET1(7), STSUFX1(12)
                const zip = row[0] || '';
                const bldgNo = row[3] || '';
                const street = row[7] || '';
                const suffix = row[12] || '';

                // Validate
                if (!this.isValidData(zip, bldgNo, street)) continue;

                // Build address
                let address = '';
                if (bldgNo) address += bldgNo.toString().trim();
                if (street) address += ' ' + street.toString().trim();
                if (suffix && suffix.toString().trim()) address += ' ' + suffix.toString().trim();
                
                address = address.trim().toUpperCase();
                if (!address || address.length < 5) continue;

                // Create simple record
                buildings.push({
                    address: address,
                    normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
                    borough: 'manhattan',
                    zipcode: zip.toString().replace(/[^0-9]/g, '').substring(0, 5) || null,
                    dhcr_source: 'csv'
                });

            } catch (error) {
                continue;
            }
        }

        // Simple deduplication
        const seen = new Set();
        const unique = buildings.filter(building => {
            const key = `${building.normalized_address}-${building.borough}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        console.log(`🔄 Deduplicated: ${buildings.length} → ${unique.length} buildings`);
        return unique;
    }

    isValidData(zip, bldgNo, street) {
        const zipStr = zip.toString().replace(/[^0-9]/g, '');
        if (zipStr.length !== 5 || (!zipStr.startsWith('10') && !zipStr.startsWith('11'))) {
            return false;
        }

        const bldgStr = bldgNo.toString().trim();
        if (!bldgStr || bldgStr.length === 0) return false;

        const streetStr = street.toString().trim();
        if (!streetStr || streetStr.length < 2) return false;

        return true;
    }
}

async function main() {
    const saver = new SimpleCSVSave();
    
    try {
        const saved = await saver.saveSimple();
        
        if (saved > 0) {
            console.log('\n🚀 NEXT STEPS:');
            console.log('1. Deploy your Railway app again');
            console.log('2. It should now find rent-stabilized buildings!');
            console.log('3. Test with: TEST_NEIGHBORHOOD=soho');
        }
        
    } catch (error) {
        console.error('💥 Simple save failed:', error.message);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}
