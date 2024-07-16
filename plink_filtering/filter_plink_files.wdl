version 1.0 

# This first task will filter the plink files by MAF and samples 
task PlinkFilterandSubset {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        File keepFile
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
        disks: "local-disk " + disk_size + " SSD"
    }

    output {
        File subset_bed = outputBed
        File subset_bim = outputBim
        File subset_fam = outputFam
    }
}

task PlinkFrequencyFilter {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        String outputPrefix = "frequency_filtered"
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
        File freq_filtered_bed = outputBed
        File freq_filtered_bim = outputBim
        File freq_filtered_fam = outputFam
    }
}

task PlinkVarMissingnessFilter {
    input {
        File sourceBed
        File sourceBim
        File sourceFam
        String outputPrefix = "missingness_filtered"
        Float varMissingness
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
        --geno ~{varMissingness} \
        --snps-only \
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