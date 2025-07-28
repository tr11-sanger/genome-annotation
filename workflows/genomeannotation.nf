/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { samplesheetToList } from 'plugin/nf-schema'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { SEQSTATS } from  '../modules/local/seqstats/main'
include { CHUNKFASTX } from  '../modules/local/chunkfastx/main'
include { CONCATENATE } from  '../modules/local/concatenate/main'
include { PYRODIGAL as PYRODIGAL_SMALL } from '../modules/nf-core/pyrodigal/main'
include { PYRODIGAL as PYRODIGAL_LARGE } from '../modules/nf-core/pyrodigal/main'
include { HMMER_HMMSEARCH } from '../modules/nf-core/hmmer/hmmsearch/main'
include { FETCHDB } from '../subworkflows/local/fetchdb/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow GENOMEANNOTATION {
    main:
    ch_versions = Channel.empty()

    // Fetch databases
    db_ch = Channel
        .from(
            params.databases.collect { k, v ->
                if ((v instanceof Map) && v.containsKey('base_dir')) {
                    return [id: k] + v
                }
            }
        )
        .filter { it }

    FETCHDB(db_ch, "${projectDir}/${params.databases.cache_path}")
    dbs_path_ch = FETCHDB.out.dbs

    dbs_path_ch
        .branch { meta, _fp ->
            pfam: meta.id == 'pfam'
        }
        .set { dbs }


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
            large: meta.base_count >= 50000 
            small: meta.base_count < 50000
        }

    PYRODIGAL_SMALL(genome_contig_split.small, 'gff')
    PYRODIGAL_LARGE(genome_contig_split.large, 'gff')

    cdss = PYRODIGAL_SMALL.out.faa
        .mix(PYRODIGAL_LARGE.out.faa)
    // Annotate CDSs
    CHUNKFASTX(cdss)
    chunked_cdss = CHUNKFASTX.out.chunked_reads.flatMap {
        meta, chunks ->
        def chunks_ = chunks instanceof Collection ? chunks : [chunks]
        return chunks_.collect {
            chunk ->
            tuple(groupKey(meta, chunks_.size()), chunk)
        }
    }

    pfam_db = dbs.pfam
        .map { meta, fp ->
            file("${fp}/${meta.base_dir}/${meta.files.hmm}")
        }
        .first()

    chunked_cdss_pfam_in = chunked_cdss
        .combine(pfam_db)
        .map { meta, seqs, db -> tuple(meta, db, seqs, false, true, true) }

    HMMER_HMMSEARCH(chunked_cdss_pfam_in)

    CONCATENATE(
        HMMER_HMMSEARCH.out.domain_summary
        .groupTuple()
        .map{ meta, results -> tuple(meta, "${meta.id}.domtbl.gz", results) }
    )

    emit:
    cds_locations = cdss
    functional_annotations = CONCATENATE.out.concatenated_file
    versions = ch_versions                 // channel: [ path(versions.yml) ]
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
