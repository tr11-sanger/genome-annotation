/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { samplesheetToList } from 'plugin/nf-schema'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { SEQSTATS } from  '../modules/local/seqstats/main'
include { PYRODIGAL as PYRODIGAL_SMALL } from '../modules/nf-core/pyrodigal/main'
include { PYRODIGAL as PYRODIGAL_LARGE } from '../modules/nf-core/pyrodigal/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow GENOMEANNOTATION {
    main:
    ch_versions = Channel.empty()

    // Parse samplesheet and fetch reads
    samplesheet = Channel.fromList(samplesheetToList(params.samplesheet, "${workflow.projectDir}/assets/schema_input.json"))

    genome_contigs = samplesheet.map {
        sample, fasta ->
        [
            ['id': sample],
            fasta,
        ]
    }

    // Get CDSs from contigs
    SEQSTATS(genome_contigs)
    genome_contig_split = SEQSTATS.out.stats
        .join(genome_contigs)
        .map { meta, stats, fasta ->
            def json = new groovy.json.JsonSlurper().parseText(stats.text)
            def meta_ = [
                'base_count': json["base_count"], 
                'seq_count': json["seq_count"]
            ]
            return tuple(meta + meta_, fasta)
        }
        .branch { meta, _fasta -> 
            large: meta.base_count >= 100000 
            small: meta.base_count < 100000
        }

    PYRODIGAL_SMALL(genome_contig_split.small, 'gff')
    PYRODIGAL_LARGE(genome_contig_split.large, 'gff')

    cdss = PYRODIGAL_SMALL.out.faa
        .mix(PYRODIGAL_LARGE.out.faa)

    emit:
    cds_locations = cdss
    versions = ch_versions                 // channel: [ path(versions.yml) ]
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
