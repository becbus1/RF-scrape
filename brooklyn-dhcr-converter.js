#!/usr/bin/env node

/**
 * BROOKLYN DHCR CSV CONVERTER (FIXED)
 * 
 * Handles Brooklyn-specific CSV format with title row and spaced headers
 */

require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const { createClient } = require('@supabase/supabase-js');
const Papa = require('papaparse');

class BrooklynDHCRConverter {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    /**
     * Convert Brooklyn DHCR CSV to Supabase format
     */
    async convertBrooklynCSV(inputPath, outputPath = null) {
        console.log('🔄 BROOKLYN DHCR CSV CONVERTER (FIXED)');
        console.log('=' .repeat(50));
        console.log(`📁 Input: ${inputPath}`);
        
        try {
            // Read CSV file
            const csvContent = await fs.readFile(inputPath, 'utf8');
            console.log(`📊 File size: ${Math.round(csvContent.length / 1024)}KB`);
            
            // Split into lines to handle the Brooklyn format manually
            const lines = csvContent.split('\n');
            console.log(`📋 Total lines: ${lines.length}`);
            
            // Find the header row (contains ZIP, BLDGNO1, etc.)
            let headerRowIndex = -1;
            for (let i = 0; i < Math.min(10, lines.length); i++) {
                if (lines[i].includes('ZIP') && lines[i].includes('BLDGNO1') && lines[i].includes('STREET1')) {
                    headerRowIndex = i;
                    console.log(`📝 Found headers at row ${i + 1}`);
                    break;
                }
            }
            
            if (headerRowIndex === -1) {
                throw new Error('Could not find DHCR header row with ZIP, BLDGNO1, STREET1');
            }
            
            // Extract header row and clean it
            const headerLine = lines[headerRowIndex];
            console.log(`📝 Header line: ${headerLine.substring(0, 100)}...`);
            
            // Parse headers and remove empty columns
            const headerParts = headerLine.split(',');
            const cleanHeaders = [];
            const headerMapping = {}; // Maps clean header names to original positions
            
            for (let i = 0; i < headerParts.length; i++) {
                const header = headerParts[i].trim();
                if (header && !cleanHeaders.includes(header)) {
                    cleanHeaders.push(header);
                    headerMapping[header] = i;
                }
            }
            
            console.log(`📝 Clean headers found: ${cleanHeaders.join(', ')}`);
            
            // Process data rows (skip title and header rows)
            const dataLines = lines.slice(headerRowIndex + 1).filter(line => line.trim());
            console.log(`📊 Data lines to process: ${dataLines.length}`);
            
            const buildings = [];
            let skipCount = 0;
            
            for (const line of dataLines) {
                try {
                    const building = this.convertBrooklynRow(line, headerMapping);
                    if (building) {
                        buildings.push(building);
                    } else {
                        skipCount++;
                    }
                } catch (error) {
                    skipCount++;
                    continue;
                }
            }
            
            console.log(`✅ Converted: ${buildings.length}, Skipped: ${skipCount}`);
            
            // Deduplicate buildings
            const uniqueBuildings = this.deduplicateBuildings(buildings);
            
            // Show sample data
            if (uniqueBuildings.length > 0) {
                console.log('\n📍 Sample converted data:');
                uniqueBuildings.slice(0, 5).forEach((building, index) => {
                    console.log(`${index + 1}. ${building.address} (${building.borough}, ${building.zipcode})`);
                });
            }
            
            // Save to file if requested
            if (outputPath) {
                await this.saveConvertedCSV(uniqueBuildings, outputPath);
            }
            
            // Save to database if requested
            if (process.env.SAVE_TO_DB === 'true') {
                await this.saveToDatabase(uniqueBuildings);
            }
            
            return uniqueBuildings;
            
        } catch (error) {
            console.error('❌ Conversion failed:', error.message);
            throw error;
        }
    }

    /**
     * Convert single Brooklyn row with manual parsing
     */
    convertBrooklynRow(line, headerMapping) {
        const parts = line.split(',');
        
        // Extract key fields using header mapping
        const zip = this.getFieldByHeader(parts, headerMapping, 'ZIP');
        const bldgNo1 = this.getFieldByHeader(parts, headerMapping, 'BLDGNO1');
        const street1 = this.getFieldByHeader(parts, headerMapping, 'STREET1');
        const suffix1 = this.getFieldByHeader(parts, headerMapping, 'STSUFX1');
        const county = this.getFieldByHeader(parts, headerMapping, 'COUNTY');
        
        // Build address
        let address = '';
        if (bldgNo1 && bldgNo1.trim()) {
            address += bldgNo1.trim();
        }
        if (street1 && street1.trim()) {
            if (address) address += ' ';
            address += street1.trim();
        }
        if (suffix1 && suffix1.trim()) {
            address += ' ' + suffix1.trim();
        }
        
        address = address.trim().toUpperCase();
        
        // Validate - Brooklyn specific validation
        if (!address || address.length < 3) {
            return null;
        }
        
        // Must have at least a street name
        if (!street1 || street1.trim().length < 2) {
            return null;
        }
        
        // Validate Brooklyn zip code
        const cleanZip = zip ? zip.toString().replace(/[^0-9]/g, '') : '';
        if (cleanZip.length !== 5 || !cleanZip.startsWith('11')) {
            return null;
        }
        
        // Validate county code (should be 61 for Brooklyn)
        const countyCode = county ? county.toString().trim() : '';
        if (countyCode !== '61') {
            console.log(`⚠️ Unexpected county code: ${countyCode} for ${address}`);
        }
        
        return {
            address: address,
            normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
            borough: 'brooklyn',
            zipcode: cleanZip,
            building_id: null,
            unit_count: null,
            registration_id: null,
            dhcr_source: 'csv',
            confidence_score: bldgNo1 ? 95 : 85,
            verification_status: 'unverified',
            parsed_at: new Date().toISOString(),
            created_at: new Date().toISOString()
        };
    }

    /**
     * Get field value by header name from parts array
     */
    getFieldByHeader(parts, headerMapping, headerName) {
        const position = headerMapping[headerName];
        if (position !== undefined && parts[position] !== undefined) {
            return parts[position].toString().trim();
        }
        return '';
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
            return await this.convertBrooklynCSV(inputPath);
        } else {
            throw new Error(`Unsupported file type: ${ext}. Please convert PDF to CSV first.`);
        }
    }
}

// Main execution
async function main() {
    const converter = new BrooklynDHCRConverter();
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        console.error('❌ Please provide a CSV file to convert');
        console.log('');
        console.log('Usage:');
        console.log('  node brooklyn-dhcr-converter.js <dhcr-file.csv>           # Convert only');
        console.log('  SAVE_TO_DB=true node brooklyn-dhcr-converter.js <file>    # Convert and save to DB');
        return;
    }
    
    const inputPath = args[0];
    const outputPath = args[1] || inputPath.replace('.csv', '-brooklyn-supabase.csv');
    
    try {
        const buildings = await converter.processFile(inputPath);
        
        console.log('\n🎉 BROOKLYN CONVERSION COMPLETE!');
        console.log('=' .repeat(50));
        console.log(`✅ Successfully converted ${buildings.length} buildings`);
        
        if (!process.env.SAVE_TO_DB) {
            console.log('');
            console.log('💡 To save to database, run:');
            console.log(`   SAVE_TO_DB=true node brooklyn-dhcr-converter.js ${inputPath}`);
        }
        
    } catch (error) {
        console.error('💥 Brooklyn conversion failed:', error.message);
        process.exit(1);
    }
}

// Export for use in other modules
module.exports = BrooklynDHCRConverter;

// Run if executed directly
if (require.main === module) {
    main();
}
