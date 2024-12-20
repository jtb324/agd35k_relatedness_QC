version 1.0

import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/plink_filtering/merge_plink_files.wdl" as merge_plink_files
import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/plink_filtering/filter_plink_files.wdl" as plink_filter
import "https://raw.githubusercontent.com/jtb324/agd35k_relatedness_QC/main/utilities/file_handling.wdl" as file_utilities

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
        Float variant_missingness = 0.1
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

        if (defined(sampleFile)) {
            call plink_filter.PlinkFilterandSubset as FilteredSubset {
                input: 
                    sourceBed=bed_file,
                    sourceBim=bim_file,
                    sourceFam=fam_file,
                    keepFile=select_first([sampleFile]),
                    outputPrefix="common_variant_filter", 
                    maf=maf, 
                    chromosome=chromosome, 
                    docker=docker,
            }
        }

        if (!defined(sampleFile)){
            call plink_filter.PlinkFrequencyFilter as FrequencyFiltered {
                input: 
                    sourceBed=bed_file,
                    sourceBim=bim_file,
                    sourceFam=fam_file,
                    outputPrefix="common_variant_filter", 
                    maf=maf, 
                    chromosome=chromosome, 
                    docker=docker,
            }
        } 

        call plink_filter.PlinkVarMissingnessFilter as PlinkMissingness {
            input:
                sourceBed=select_first([FilteredSubset.subset_bed,FrequencyFiltered.freq_filtered_bed]),
                sourceBim=select_first([FilteredSubset.subset_bim,FrequencyFiltered.freq_filtered_bim]),
                sourceFam=select_first([FilteredSubset.subset_fam,FrequencyFiltered.freq_filtered_fam]),
                varMissingness=variant_missingness,
                chromosome=chromosome,
                docker=docker
        }
    } 

    call merge_plink_files.MergePlinkFiles as MergePlinkFiles { 
        input: 
        bed_files=PlinkMissingness.output_bed, 
        bim_files=PlinkMissingness.output_bim, 
        fam_files=PlinkMissingness.output_fam, 
        target_prefix=plinkOutputPrefix
        }

    if(defined(target_gcp_folder)){
        call file_utilities.MoveOrCopyFourFiles {
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
