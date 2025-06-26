#!/usr/bin/env node

/**
 * QUICK CSV FIX - Save Manhattan DHCR buildings directly
 * 
 * This will take your Manhattan CSV and save it to Supabase immediately
 */

require('dotenv').config();
const fs = require('fs').promises;
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class QuickCSVFix {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    async fixManhattanCSV() {
        console.log('🔧 QUICK FIX: Manhattan DHCR CSV → Supabase');
        console.log('=' .repeat(50));

        try {
            // Read the Manhattan CSV
            const csvPath = 'data/dhcr/2023-DHCR-Bldg-File-Manhattan.csv';
            console.log(`📁 Reading: ${csvPath}`);
            
            const csvContent = await fs.readFile(csvPath, 'utf8');
            console.log(`📊 File size: ${Math.round(csvContent.length / 1024)}KB`);

            // Parse CSV
            const parsed = Papa.parse(csvContent, {
                header: true,
                skipEmptyLines: true,
                transformHeader: (header) => header.trim()
            });

            console.log(`📋 Rows parsed: ${parsed.data.length}`);
            console.log(`📝 Headers:`, parsed.meta.fields?.slice(0, 10));

            // Convert to Supabase format
            const buildings = this.convertToSupabaseFormat(parsed.data);
            console.log(`✅ Converted: ${buildings.length} buildings`);

            // Show sample
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

    convertToSupabaseFormat(csvData) {
        const buildings = [];
        
        for (const row of csvData) {
            try {
                // Extract data from CSV columns
                const zip = row.ZIP || row.zip || '';
                const bldgNo1 = row.BLDGNO1 || row.bldgno1 || '';
                const street1 = row.STREET1 || row.street1 || '';
                const suffix1 = row.STSUFX1 || row.stsufx1 || '';

                // Build address
                let address = '';
                if (bldgNo1) address += bldgNo1.toString().trim();
                if (street1) address += ' ' + street1.toString().trim();
                if (suffix1) address += ' ' + suffix1.toString().trim();
                
                address = address.trim().toUpperCase();

                // Skip if invalid
                if (!address || address.length < 5 || !bldgNo1) {
                    continue;
                }

                // Create building record
                const building = {
                    address: address,
                    normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
                    borough: 'manhattan', // All from Manhattan CSV
                    zipcode: zip.toString().substring(0, 5),
                    building_id: null,
                    unit_count: null,
                    registration_id: null,
                    dhcr_source: 'csv',
                    confidence_score: 95,
                    verification_status: 'unverified',
                    parsed_at: new Date().toISOString(),
                    created_at: new Date().toISOString()
                };

                buildings.push(building);

            } catch (error) {
                // Skip invalid rows
                continue;
            }
        }

        // Remove duplicates
        return this.deduplicateBuildings(buildings);
    }

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
                    console.error(`📝 Error details:`, error);
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
    const fixer = new QuickCSVFix();
    
    try {
        const saved = await fixer.fixManhattanCSV();
        
        if (saved > 0) {
            console.log('\n🚀 NEXT STEPS:');
            console.log('1. Deploy your Railway app again');
            console.log('2. It should now find rent-stabilized buildings!');
            console.log('3. Test with: TEST_NEIGHBORHOOD=soho');
        }
        
    } catch (error) {
        console.error('💥 Quick fix failed:', error.message);
        process.exit(1);
    }
}

// Run if executed directly
if (require.main === module) {
    main();
}
