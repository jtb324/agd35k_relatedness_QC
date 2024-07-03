version 1.0

import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/plink_common_variant_filter.wdl" as plink_filter_utils
import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils
import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/excess_heterozygosity/calculate_excess_heterozygosity.wdl" as het_utils 

workflow RunsOfHomozygosity {
    input  {
        # We first need to provide arrays of the bed/bim/fam files since we are using plink1.9 formatted files
        Array[File] source_bed_files 
        Array[File] source_bim_files
        Array[File] source_fam_files

        Float maf
        Float variant_missingness

        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        String outputPrefix = "test"

        Array[String] chroms

        String? project_id
        # We have to specify an output bucket if we want to store the output in a folder
        String target_gcp_folder
    }

    scatter (indx in range(length(chroms))) {

        String chromosome = chroms[indx]

        File bed_file = source_bed_files[indx]
        File bim_file = source_bim_files[indx]
        File fam_file = source_fam_files[indx]

        call het_utils.PlinkFilter as Filter{
            input: 
            sourceBed=bed_file,
            sourceBim=bim_file,
            sourceFam=fam_file,
            filteredOutputPrefix="common_variant_filter", 
            maf=maf, 
            variant_missingness=variant_missingness,
            chromosome=chromosome, 
            docker=docker,
        }

        call DetermineHomozygosityRuns {
            input:
            sourceBed=Filter.output_bed,
            sourceBim=Filter.output_bim,
            sourceFam=Filter.output_fam,
            outputPrefix=outputPrefix,
            chromosome=chromosome
        }
        call GcpUtils.MoveOrCopyThreeFiles {
            input:
            source_file1=DetermineHomozygosityRuns.outputHom,
            source_file2=DetermineHomozygosityRuns.outputHomInd,
            source_file3=DetermineHomozygosityRuns.outputHomSummary,
            target_gcp_folder=target_gcp_folder
        }
    }
}
task DetermineHomozygosityRuns {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        String outputPrefix
        String chromosome
        Int memory_gb = 20
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    String homFileName = "${outputPrefix}_${chromosome}.hom"
    String indFileName = "${outputPrefix}_${chromosome}.hom.indiv"
    String summaryFileName = "${outputPrefix}_${chromosome}.hom.summary"

    String chrOutputPrefix = "${outputPrefix}_${chromosome}"

    # This line allows us to dynamically allocate memory at runtime so we don't hit an out of memory error
    Int disk_size = ceil((size(sourceBed, "GB") + size(sourceBim, "GB") + size(sourceFam, "GB"))  * 3) + 20

    command <<<
        plink \
        --bed ~{sourceBed} \
        --bim ~{sourceBim} \
        --fam ~{sourceFam} \
        --homozyg \
        --out ~{chrOutputPrefix}
    >>>

    runtime {
        docker: docker
        memory: memory_gb + "GiB"
        disks: "local-disk " + disk_size + " SSD"
    }

    output {
        File outputHom = homFileName
        File outputHomInd = indFileName
        File outputHomSummary = summaryFileName
    }
}