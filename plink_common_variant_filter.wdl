version 1.0

workflow commonVariantFilter {
    input {
        # We first need to provide arrays of the bed/bim/fam files since we are using plink1.9 formatted files
        Array[File] source_bed_files 
        Array[File] source_bim_files
        Array[File] source_fam_files
        # Then we are going to provide an value for the output plink files
        String plinkOutputPrefix = "test"
        # The goal with this workflow is to filter down the variants to a more manageable size. This will be done with a MAF filter
        Float maf
        # We can just use the standard plink docker image for most things in this workflow
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        # We are also going to subset the file to a smaller number of individuals (n=5,000). This sampling will help with runtime memory
        File? sampleFile
        # This is just the array containing the chromosome numbers from the file arrays. This is an attribute in the Terra GUI
        Array[String] chroms
        String? project_id
        # We have to specify an output bucket if we want to store the output in a folder
        String? target_gcp_folder
    }

    # This process can be parallelized over all the chromosomes
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
            keepFile=sampleFile, 
            outputPrefix="common_variant_filter", 
            maf=maf, 
            chromosome=chromosome, 
            docker=docker,
        }

    } 

    call MergePlinkFiles { input: bed_files=PlinkFilter.output_bed, bim_files=PlinkFilter.output_bim, fam_files=PlinkFilter.output_fam, target_prefix=plinkOutputPrefix}

    if(defined(target_gcp_folder)){
        call MoveOrCopyFourFiles {
            input:
                source_file1 = MergePlinkFiles.output_merged_bed,
                source_file2 = MergePlinkFiles.output_merged_bim,
                source_file3 = MergePlinkFiles.output_merged_fam,
                source_file4 = MergePlinkFiles.output_merged_log,
                is_move_file = false,
                project_id = project_id,
                target_gcp_folder = select_first([target_gcp_folder])
            }
        }
}


# This first task will filter the plink files by MAF and samples 
task PlinkFilter {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        File? keepFile
        String outputPrefix
        Float maf
        String chromosome
        Int memory_gb = 20
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    String outputBed = "${outputPrefix}_${chromosome}.bed"
    String outputBim = "${outputPrefix}_${chromosome}.bim"
    String outputFam = "${outputPrefix}_${chromosome}.fam"

    String chrOutputPrefix = "${outputPrefix}_${chromosome}"

    # This line allows us to dynamically allocate memory at runtime so we don't hit an out of memory error
    Int disk_size = ceil((size(sourceBed, "GB") + size(sourceBim, "GB") + size(sourceFam, "GB"))  * 3) + 20

    command <<< 
        plink2 \
        --bed ~{sourceBed} \
        --bim ~{sourceBim} \
        --fam ~{sourceFam} \
        --maf ~{maf} \
        --snps-only \
        --keep ~{keepFile} \
        --set-all-var-ids "chr@:#:\$r:\$a" \
        --new-id-max-allele-len 1000 \
        --make-bed \
        --out ~{chrOutputPrefix}
    >>>

    runtime {
        docker: docker
        memory:  memory_gb + "GiB"
        disks: "local-disk " + disk_size + " HDD"
    }

    output {
        File output_bed = outputBed
        File output_bim = outputBim
        File output_fam = outputFam
    }
}

# This second task will merge all of the different filtered plink files into one
# This task was adapted from the "MergePgenFiles" to work with bed/bim/fam files. 
# The original task is found here: https://raw.githubusercontent.com/shengqh/warp/develop/pipelines/vumc_biostatistics/genotype/Utils.wdl
task MergePlinkFiles {
    input {
        Array[File] bed_files
        Array[File] bim_files
        Array[File] fam_files

        String target_prefix

        Int memory_gb = 20

        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    Int disk_size = ceil((size(bed_files, "GB") + size(bim_files, "GB") + size(fam_files, "GB"))  * 3) + 20


    String merged_bed = target_prefix + ".bed"
    String merged_bim = target_prefix + ".bim"
    String merged_fam = target_prefix + ".fam"
    String merged_log = target_prefix + ".log"

    command <<<

    cat ~{write_lines(bed_files)} > bed.list
    cat ~{write_lines(bim_files)} > bim.list
    cat ~{write_lines(fam_files)} > fam.list

    paste bed.list bim.list fam.list > merge.list

    plink2 --pmerge-list merge.list --make-bed --out ~{target_prefix} --delete-pmerge-result

    >>>

    runtime {
        docker: docker
        preemptible: 0
        disks: "local-disk " + disk_size + " HDD"
        memory: memory_gb + " GiB"
    }

    output {
        File output_merged_bed = merged_bed
        File output_merged_bim = merged_bim
        File output_merged_fam = merged_fam
        File output_merged_log = merged_log
    }
}

# The third task is used to copy all of the output from the plink merging step into the GCP project bucket.
# This task is adapted from the "MoveOrCopyThreeFiles" found here: https://raw.githubusercontent.com/shengqh/warp/develop/tasks/vumc_biostatistics/GcpUtils.wdl
task MoveOrCopyFourFiles {
    input {
        File source_file1
        File source_file2
        File source_file3
        File source_file4

        Boolean is_move_file = false

        Int memory = 2
        String? project_id
        String target_gcp_folder
    }

    String action = if (is_move_file) then "mv" else "cp"

    String gcs_output_dir = sub(target_gcp_folder, "/+$", "")

    String new_file1 = "~{gcs_output_dir}/~{basename(source_file1)}"
    String new_file2 = "~{gcs_output_dir}/~{basename(source_file2)}"
    String new_file3 = "~{gcs_output_dir}/~{basename(source_file3)}"
    String new_file4 = "~{gcs_output_dir}/~{basename(source_file4)}"

    command <<<

    set -e

    gsutil -m ~{"-u " + project_id} ~{action} ~{source_file1} ~{source_file2} ~{source_file3} ~{source_file4} ~{gcs_output_dir}/

    >>>

    runtime {
        docker: "google/cloud-sdk"
        preemptible: 1
        disks: "local-disk 10 HDD"
        memory: memory + "GiB"
    }
    output {
        String output_file1 = new_file1
        String output_file2 = new_file2
        String output_file3 = new_file3
        String output_file4 = new_file4
    }
}