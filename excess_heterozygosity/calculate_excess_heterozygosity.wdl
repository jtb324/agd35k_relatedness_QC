version 1.0

import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/plink_common_variant_filter.wdl" as plink_filter_utils
import "https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl" as GcpUtils

workflow PlinkExcessHeterozygosity {
    input {
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

        call PlinkFilter {
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
    }
    # Merge all of the filtered plink files into one set of files
    call plink_filter_utils.MergePlinkFiles as MergePlinkFiles {
        input: 
        bed_files=PlinkFilter.output_bed, 
        bim_files=PlinkFilter.output_bim,
        fam_files=PlinkFilter.output_fam,
        target_prefix="merged_filtered_files"
    }

    call calculateExcessHeterozygosity {
        input:
        sourceBed=MergePlinkFiles.output_merged_bed,
        sourceBim=MergePlinkFiles.output_merged_bim,
        sourceFam=MergePlinkFiles.output_merged_fam, 
        outputPrefix=outputPrefix
    }

    call GcpUtils.MoveOrCopyTwoFiles {
        input:
        source_file1=calculateExcessHeterozygosity.output_het_file,
        source_file2=calculateExcessHeterozygosity.output_log_file,
        target_gcp_folder=target_gcp_folder
    }
}

# This first task will filter the plink files by MAF and samples 
task PlinkFilter {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        String filteredOutputPrefix
        Float maf
        Float variant_missingness
        String chromosome
        Int memory_gb = 20
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    String outputBed = "${filteredOutputPrefix}_${chromosome}.bed"
    String outputBim = "${filteredOutputPrefix}_${chromosome}.bim"
    String outputFam = "${filteredOutputPrefix}_${chromosome}.fam"

    String chrOutputPrefix = "${filteredOutputPrefix}_${chromosome}"

    # This line allows us to dynamically allocate memory at runtime so we don't hit an out of memory error
    Int disk_size = ceil((size(sourceBed, "GB") + size(sourceBim, "GB") + size(sourceFam, "GB"))  * 3) + 20

    command <<< 
        plink2 \
        --bed ~{sourceBed} \
        --bim ~{sourceBim} \
        --fam ~{sourceFam} \
        --maf ~{maf} \
        --snps-only \
        --geno ~{variant_missingness}
        --set-all-var-ids "chr@:#:\$r:\$a" \
        --new-id-max-allele-len 1000 \
        --make-bed \
        --out ~{chrOutputPrefix}
    >>>

    runtime {
        docker: docker
        memory:  memory_gb + "GiB"
        disks: "local-disk " + disk_size + " SSD"
    }

    output {
        File output_bed = outputBed
        File output_bim = outputBim
        File output_fam = outputFam
    }
}

task calculateExcessHeterozygosity {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        String outputPrefix
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        Int? memory_gb = 20
    }

    Int disk_size = ceil((size(sourceBed, "GB") + size(sourceBim, "GB") + size(sourceFam, "GB"))  * 3) + 20

    String outputHetFile = "${outputPrefix}.het"
    String outputHetLog = "${outputPrefix}.log"

    command <<<
        plink2 \
        --bed ~{sourceBed} \
        --bim ~{sourceBim} \
        --fam ~{sourceFam} \
        --het \
        --set-all-var-ids "chr@:#:\$r:\$a" \
        --new-id-max-allele-len 1000 \
        --snps-only \
        --out ~{outputPrefix}
    >>>

    runtime {
        docker: docker
        memory:  memory_gb + "GiB"
        disks: "local-disk " + disk_size + " SSD"
    }
    output {
        File output_het_file=outputHetFile
        File output_log_file=outputHetLog
    }
}