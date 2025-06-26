#!/usr/bin/env node

/**
 * DHCR CSV CONVERTER
 * 
 * Converts DHCR CSV format to Supabase format
 * 
 * DHCR Format:
 * ZIP BLDGNO1 STREET1 STSUFX1 BLDGNO2 STREET2 STSUFX2 COUNTY CITY STATUS1 STATUS2 STATUS3 BLOCK LOT
 * 
 * Supabase Format:
 * address, normalized_address, borough, zipcode, building_id, dhcr_source, confidence_score
 */

require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class DHCRCSVConverter {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    /**
     * Convert DHCR CSV to Supabase format
     */
    async convertDHCRCSV(inputPath, outputPath = null) {
        console.log('🔄 DHCR CSV CONVERTER');
        console.log('=' .repeat(50));
        console.log(`📁 Input: ${inputPath}`);
        
        try {
            // Read CSV file
            const csvContent = await fs.readFile(inputPath, 'utf8');
            console.log(`📊 File size: ${Math.round(csvContent.length / 1024)}KB`);
            
            // Parse CSV
            const parsed = Papa.parse(csvContent, {
                header: true,
                skipEmptyLines: true,
                delimiter: ',', // Try comma first
                transformHeader: (header) => header.trim().toUpperCase()
            });
            
            console.log(`📋 Parsed ${parsed.data.length} rows`);
            console.log(`📝 Headers found:`, parsed.meta.fields?.slice(0, 10) || 'No headers detected');
            
            if (parsed.errors.length > 0) {
                console.log(`⚠️ Parsing warnings:`, parsed.errors.slice(0, 3));
            }
            
            // Try different delimiters if comma didn't work well
            if (parsed.data.length === 0 || !this.hasExpectedDHCRColumns(parsed.meta.fields)) {
                console.log('🔄 Trying tab delimiter...');
                
                const tabParsed = Papa.parse(csvContent, {
                    header: true,
                    skipEmptyLines: true,
                    delimiter: '\t',
                    transformHeader: (header) => header.trim().toUpperCase()
                });
                
                if (tabParsed.data.length > parsed.data.length) {
                    console.log(`✅ Tab delimiter worked better: ${tabParsed.data.length} rows`);
                    parsed.data = tabParsed.data;
                    parsed.meta = tabParsed.meta;
                }
            }
            
            // Convert to Supabase format
            const converted = this.convertToSupabaseFormat(parsed.data, parsed.meta.fields);
            console.log(`✅ Converted ${converted.length} buildings`);
            
            // Show sample data
            if (converted.length > 0) {
                console.log('\n📍 Sample converted data:');
                converted.slice(0, 5).forEach((building, index) => {
                    console.log(`${index + 1}. ${building.address} (${building.borough}, ${building.zipcode})`);
                });
            }
            
            // Save to file if requested
            if (outputPath) {
                await this.saveConvertedCSV(converted, outputPath);
            }
            
            // Save to database if requested
            if (process.env.SAVE_TO_DB === 'true') {
                await this.saveToDatabase(converted);
            }
            
            return converted;
            
        } catch (error) {
            console.error('❌ Conversion failed:', error.message);
            throw error;
        }
    }

    /**
     * Check if CSV has expected DHCR columns
     */
    hasExpectedDHCRColumns(headers) {
        if (!headers) return false;
        
        const expectedColumns = ['ZIP', 'BLDGNO1', 'STREET1', 'COUNTY'];
        const headerStr = headers.join(' ').toUpperCase();
        
        return expectedColumns.some(col => headerStr.includes(col));
    }

    /**
     * Convert DHCR data to Supabase format
     */
    convertToSupabaseFormat(data, headers) {
        console.log('🔄 Converting to Supabase format...');
        
        const converted = [];
        let skipCount = 0;
        
        for (const row of data) {
            try {
                const building = this.convertSingleRow(row);
                
                if (building && building.address && building.address.length > 5) {
                    converted.push(building);
                } else {
                    skipCount++;
                }
                
            } catch (error) {
                skipCount++;
                continue;
            }
        }
        
        console.log(`   ✅ Converted: ${converted.length}, Skipped: ${skipCount}`);
        return this.deduplicateBuildings(converted);
    }

    /**
     * Convert single DHCR row to Supabase format
     */
    convertSingleRow(row) {
        // Handle different possible column names/formats
        const zip = this.getFieldValue(row, ['ZIP', 'ZIPCODE', 'ZIP_CODE']);
        const bldgNo1 = this.getFieldValue(row, ['BLDGNO1', 'BLDG_NO1', 'BUILDING_NUMBER', 'HOUSE_NUMBER']);
        const street1 = this.getFieldValue(row, ['STREET1', 'STREET_1', 'STREET_NAME', 'STREET']);
        const suffix1 = this.getFieldValue(row, ['STSUFX1', 'ST_SUFX1', 'STREET_SUFFIX', 'SUFFIX']);
        const county = this.getFieldValue(row, ['COUNTY', 'BORO', 'BOROUGH']);
        const city = this.getFieldValue(row, ['CITY']);
        
        // Build address from components
        let address = '';
        
        if (bldgNo1) {
            address += bldgNo1.toString().trim();
        }
        
        if (street1) {
            address += ' ' + street1.toString().trim();
        }
        
        if (suffix1) {
            address += ' ' + suffix1.toString().trim();
        }
        
        address = address.trim().toUpperCase();
        
        // Determine borough
        const borough = this.normalizeBoroughName(county || city || '');
        
        // Validate required fields
        if (!address || address.length < 5) {
            return null;
        }
        
        return {
            address: address,
            normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
            borough: borough,
            zipcode: this.normalizeZipcode(zip),
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
     * Get field value trying multiple possible column names
     */
    getFieldValue(row, possibleNames) {
        for (const name of possibleNames) {
            if (row[name] !== undefined && row[name] !== null && row[name] !== '') {
                return row[name];
            }
        }
        return '';
    }

    /**
     * Normalize borough name
     */
    normalizeBoroughName(borough) {
        if (!borough) return 'manhattan';
        
        const normalized = borough.toString().toLowerCase().trim();
        
        const boroughMap = {
            'manhattan': 'manhattan',
            'new york': 'manhattan',
            'ny': 'manhattan',
            '1': 'manhattan',
            'brooklyn': 'brooklyn',
            'kings': 'brooklyn',
            'bk': 'brooklyn',
            'bklyn': 'brooklyn',
            '2': 'brooklyn',
            'queens': 'queens',
            'qns': 'queens',
            '3': 'queens',
            'bronx': 'bronx',
            'bx': 'bronx',
            '4': 'bronx',
            'staten island': 'staten_island',
            'richmond': 'staten_island',
            'si': 'staten_island',
            '5': 'staten_island'
        };
        
        return boroughMap[normalized] || 'manhattan';
    }

    /**
     * Normalize zipcode
     */
    normalizeZipcode(zipcode) {
        if (!zipcode) return '';
        
        const zip = zipcode.toString().replace(/[^0-9]/g, '');
        return zip.length >= 5 ? zip.substring(0, 5) : zip;
    }

    /**
     * Remove duplicate buildings
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
        
        console.log(`   🔄 Deduplicated: ${buildings.length} → ${unique.length} buildings`);
        return unique;
    }

    /**
     * Save converted data to new CSV file
     */
    async saveConvertedCSV(buildings, outputPath) {
        console.log(`💾 Saving converted CSV to: ${outputPath}`);
        
        const csv = Papa.unparse(buildings, {
            header: true,
            columns: [
                'address',
                'normalized_address', 
                'borough',
                'zipcode',
                'dhcr_source',
                'confidence_score'
            ]
        });
        
        await fs.writeFile(outputPath, csv);
        console.log(`✅ Saved ${buildings.length} buildings to ${outputPath}`);
    }

    /**
     * Save to Supabase database
     */
    async saveToDatabase(buildings) {
        if (buildings.length === 0) {
            console.log('📊 No buildings to save to database');
            return;
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
                        onConflict: 'normalized_address,borough',
                        ignoreDuplicates: false 
                    });
                
                if (error) {
                    console.error(`   ❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
                    continue;
                }
                
                saved += batch.length;
                console.log(`   ✅ Saved batch ${Math.floor(i/batchSize) + 1}: ${saved}/${buildings.length}`);
            }
            
            console.log(`🎉 Successfully saved ${saved} buildings to database!`);
            return saved;
            
        } catch (error) {
            console.error('❌ Database save failed:', error.message);
            throw error;
        }
    }

    /**
     * Auto-detect and process DHCR file
     */
    async processFile(inputPath) {
        console.log(`🔍 Processing file: ${path.basename(inputPath)}`);
        
        const ext = path.extname(inputPath).toLowerCase();
        
        if (ext === '.csv') {
            return await this.convertDHCRCSV(inputPath);
        } else {
            throw new Error(`Unsupported file type: ${ext}. Please convert PDF to CSV first.`);
        }
    }
}

// Manual CSV Creation Guide
function showManualConversionGuide() {
    console.log('📋 MANUAL DHCR PDF → CSV CONVERSION GUIDE');
    console.log('=' .repeat(50));
    console.log('');
    console.log('Since PDF parsing failed, follow these steps:');
    console.log('');
    console.log('1. 📄 Open your DHCR PDF file');
    console.log('   - Use Adobe Acrobat, browser, or PDF viewer');
    console.log('');
    console.log('2. 🔄 Convert to CSV:');
    console.log('   Option A: Adobe Acrobat → Export Data → Spreadsheet (CSV)');
    console.log('   Option B: Copy/paste into Google Sheets → Download as CSV');
    console.log('   Option C: Use online PDF to CSV converter');
    console.log('');
    console.log('3. ✅ Expected CSV format:');
    console.log('   ZIP,BLDGNO1,STREET1,STSUFX1,BLDGNO2,STREET2,STSUFX2,COUNTY,CITY,STATUS1,STATUS2,STATUS3,BLOCK,LOT');
    console.log('   10012,123,SPRING,ST,,,NEW YORK,MANHATTAN,...');
    console.log('');
    console.log('4. 🚀 Convert with this tool:');
    console.log('   node dhcr-csv-converter.js your-dhcr-file.csv');
    console.log('');
    console.log('5. 💾 Save to database:');
    console.log('   SAVE_TO_DB=true node dhcr-csv-converter.js your-dhcr-file.csv');
    console.log('');
    console.log('📁 Sample files to convert:');
    console.log('   - 2023-DHCR-Bldg-File-Manhattan.pdf → manhattan-dhcr.csv');
    console.log('   - 2023-DHCR-Bldg-File-Brooklyn.pdf → brooklyn-dhcr.csv');
    console.log('   - 2023-DHCR-Bldg-File-Queens.pdf → queens-dhcr.csv');
    console.log('   - 2023-DHCR-Bldg-File-Bronx.pdf → bronx-dhcr.csv');
}

// Main execution
async function main() {
    const converter = new DHCRCSVConverter();
    const args = process.argv.slice(2);
    
    if (args.includes('--help') || args.includes('--guide')) {
        showManualConversionGuide();
        return;
    }
    
    if (args.length === 0) {
        console.error('❌ Please provide a CSV file to convert');
        console.log('');
        console.log('Usage:');
        console.log('  node dhcr-csv-converter.js <dhcr-file.csv>           # Convert only');
        console.log('  SAVE_TO_DB=true node dhcr-csv-converter.js <file>    # Convert and save to DB');
        console.log('  node dhcr-csv-converter.js --guide                   # Show conversion guide');
        console.log('');
        showManualConversionGuide();
        return;
    }
    
    const inputPath = args[0];
    const outputPath = args[1] || inputPath.replace('.csv', '-supabase.csv');
    
    try {
        const buildings = await converter.processFile(inputPath);
        
        console.log('\n🎉 CONVERSION COMPLETE!');
        console.log('=' .repeat(50));
        console.log(`✅ Successfully converted ${buildings.length} buildings`);
        
        if (!process.env.SAVE_TO_DB) {
            console.log('');
            console.log('💡 To save to database, run:');
            console.log(`   SAVE_TO_DB=true node dhcr-csv-converter.js ${inputPath}`);
        }
        
    } catch (error) {
        console.error('💥 Conversion failed:', error.message);
        console.log('');
        console.log('💡 Need help? Run:');
        console.log('   node dhcr-csv-converter.js --guide');
        process.exit(1);
    }
}

// Export for use in other modules
module.exports = DHCRCSVConverter;

// Run if executed directly
if (require.main === module) {
    main();
}
