version 1.0

import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/plink_common_variant_filter.wdl" as plink_filter_utils

workflow ExtractPlinkRegion {
    input {
        File bedFile
        File bimFile
        File famFile
        
        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        String outputPrefix = "test"

        File regionBim
        File keepFile

        String? project_id
        # We have to specify an output bucket if we want to store the output in a folder
        String target_gcp_folder
    }

    call ExtractRegion {
        input: 
        inputBedFile=bedFile,
        inputBimFile=bimFile,
        inputFamFile=famFile,
        outputName=outputPrefix,
        regionBim=regionBim,
        keepFile=keepFile
    }

    if (defined(target_gcp_folder)) {
        call plink_filter_utils.MoveOrCopyFourFiles {
        input: 
            source_file1= ExtractRegion.outputBed,
            source_file2= ExtractRegion.outputBim,
            source_file3= ExtractRegion.outputFam,
            source_file4= ExtractRegion.outputRaw,
            target_gcp_folder = select_first([target_gcp_folder]),
            memory=5
        }
    }
}

task ExtractRegion {
    input {
        File inputBedFile
        File inputBimFile
        File inputFamFile
        File regionBim
        File keepFile

        String docker = "hkim298/plink_1.9_2.0:20230116_20230707"
        String outputName = "test"

        Int memory_gb = 5
    }

    String newBed = "${outputName}.bed"
    String newBim = "${outputName}.bim"
    String newFam = "${outputName}.fam"
    String newRaw = "${outputName}.raw"
    String newLog = "${outputName}.log"

    Int disk_size = ceil((size(inputBedFile, "GB") + size(inputBimFile, "GB") + size(inputFamFile, "GB"))  * 3) + 20

    command <<< 
        plink2 \
        --bed ~{inputBedFile} \
        --bim ~{inputBimFile} \
        --fam ~{inputFamFile} \
        --extract bed0 ~{regionBim} \
        --keep ~{keepFile} \
        --set-all-var-ids "chr@:#:\$r:\$a" \
        --new-id-max-allele-len 1000 \
        --make-bed \
        --export A \
        --out ~{outputName}
    >>>

    runtime {
        docker: docker
        memory:  memory_gb + "GiB"
        disks: "local-disk " + disk_size + " SSD"
    }

    output {
        File outputBed = newBed
        File outputBim = newBim
        File outputFam = newFam
        File outputLog = newLog
        File outputRaw = newRaw
    }
}
