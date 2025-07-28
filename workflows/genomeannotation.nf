/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { samplesheetToList } from 'plugin/nf-schema'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { CHUNKFASTX as CHUNKFASTX_CONTIG } from  '../modules/local/chunkfastx/main'
include { CHUNKFASTX as CHUNKFASTX_CDS } from  '../modules/local/chunkfastx/main'
include { CONCATENATE as CONCATENATE_CDS } from  '../modules/local/concatenate/main'
include { CONCATENATE as CONCATENATE_FAA } from  '../modules/local/concatenate/main'
include { CONCATENATE as CONCATENATE_DOMTBL } from  '../modules/local/concatenate/main'
include { PYRODIGAL } from '../modules/nf-core/pyrodigal/main'
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
    CHUNKFASTX_CONTIG(genome_contigs)
    chunked_genome_contigs = CHUNKFASTX_CONTIG.out.chunked_reads.flatMap {
        meta, chunks ->
        def chunks_ = chunks instanceof Collection ? chunks : [chunks]
        return chunks_.collect {
            chunk ->
            tuple(groupKey(meta, chunks_.size()), chunk)
        }
    }

    PYRODIGAL(chunked_genome_contigs, 'gff')

    CONCATENATE_CDS(PYRODIGAL.out.annotations.groupTuple())
    CONCATENATE_FAA(PYRODIGAL.out.faa.groupTuple())
    cdss = CONCATENATE_FAA.out.concatenated_file

    // Annotate CDSs
    CHUNKFASTX_CDS(cdss)
    chunked_cdss = CHUNKFASTX_CDS.out.chunked_reads.flatMap {
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

    CONCATENATE_DOMTBL(HMMER_HMMSEARCH.out.domain_summary.groupTuple())

    emit:
    cds_locations = CONCATENATE_FAA.out.concatenated_file
    functional_annotations = CONCATENATE_DOMTBL.out.concatenated_file
    versions = ch_versions                 // channel: [ path(versions.yml) ]
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
