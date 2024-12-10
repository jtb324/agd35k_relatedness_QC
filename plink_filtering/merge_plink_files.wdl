version 1.0

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
        disks: "local-disk " + disk_size + " SSD"
        memory: memory_gb + " GiB"
    }

    output {
        File output_merged_bed = merged_bed
        File output_merged_bim = merged_bim
        File output_merged_fam = merged_fam
        File output_merged_log = merged_log
    }
}

task MergePlink2Files {
    input {
        Array[File] pgen_files
        Array[File] pvar_files
        Array[File] psam_files

        String target_prefix

        Int memory_gb = 20

        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
    }

    Int disk_size = ceil((size(pgen_files, "GB") + size(pvar_files, "GB") + size(psam_files, "GB"))  * 3) + 20


    String merged_pgen = target_prefix + ".pgen"
    String merged_psam = target_prefix + ".psam"
    String merged_pvar = target_prefix + ".pvar"
    String merged_log = target_prefix + ".log"

    command <<<

    cat ~{write_lines(pgen_files)} > pgen.list
    cat ~{write_lines(psam_files)} > psam.list
    cat ~{write_lines(pvar_files)} > pvar.list

    paste pgen.list psam.list pvar.list > merge.list

    plink2 --pmerge-list merge.list --make-pgen --out ~{target_prefix} --delete-pmerge-result

    >>>

    runtime {
        docker: docker
        preemptible: 0
        disks: "local-disk " + disk_size + " SSD"
        memory: memory_gb + " GiB"
    }

    output {
        File output_merged_pgen = merged_pgen
        File output_merged_psam = merged_psam
        File output_merged_pvar = merged_pvar
        File output_merged_log = merged_log
    }
}