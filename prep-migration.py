#!/usr/bin/env python3

import json
import os
import csv
from argparse import ArgumentParser
from collections import OrderedDict
from datetime import date


def prep_migrate_payloads(song_dump, source_metadata_files, env):
    date_str = date.today().strftime("%Y-%m-%d")
    output_dir = os.path.join(os.getcwd(), 'virusseq_song_update', env, date_str)
    if not os.path.exists(output_dir):
      os.makedirs(output_dir)
    studyId = os.path.basename(song_dump).split(".")[1]
    migrate_dump = os.path.join(output_dir, studyId+'.migrate_consensus_sequence.jsonl')

    annotation = {}
    field_update = {
                    # 'sample collection date': 'sample_collection_date'
                    # 'sequencing instrument': 'sequencing_instrument', 
                    # 'sequencing protocol': 'sequencing_protocol', 
                    # 'raw sequence data processing method': 'raw_sequence_data_processing_method', 
                    # 'dehosting method': 'dehosting_method', 
                    # 'consensus sequence software name': 'consensus_sequence_software_name', 
                    # 'consensus sequence software version': 'consensus_sequence_software_version', 
                    # 'bioinformatics protocol': 'bioinformatics_protocol', 
                    # 'gene name': 'gene_name', 
                    # 'diagnostic pcr Ct value': 'diagnostic_pcr_ct_value',
                    # 'diagnostic pcr Ct value null reason': 'diagnostic_pcr_ct_value_null_reason',
                    # "purpose of sequencing": 'purpose_of_sequencing',
                    # 'host age': 'host_age',
                    # 'host age null reason': 'host_age_null_reason',
                    # 'host age unit': 'host_age_unit',
                    # 'host age bin': 'host_age_bin',
                    # 'GISAID accession': 'gisaid_accession',
                    'purpose of sampling': 'purpose_of_sampling'
                    }
    if source_metadata_files:
      for fl in source_metadata_files:
        with open(fl, 'r', encoding='utf-8-sig') as fp:
          reader = csv.DictReader(fp, delimiter='\t')
          for line in reader:
            key = line.get('specimen collector sample ID')
            if not key in annotation: annotation[key] = {}
            for fn in field_update:
              if not fn in line: continue
              if not line.get(fn):
                fn_value = None
              else:
                fn_value = line.get(fn).strip()
              if fn in annotation[key] and not annotation[key][fn] == fn_value:
                print(f'Different values in field {fn}: using {annotation[key][fn]} instead of {fn_value} for specimen collector sample ID {key}')
                continue 
              annotation[key].update({fn: fn_value})    

    #print(annotation)
    with open(migrate_dump, 'w') as fm:
      with open(song_dump, 'r') as fp:
        for fline in fp:
          analysis = json.loads(fline)
          sampleId = analysis['samples'][0]['submitterSampleId']
          if not sampleId in annotation: continue
          analysis_state = analysis.pop('analysisState')
          if not analysis_state == 'PUBLISHED': continue
          analysis['analysisType'].pop('version')
          for item in ['createdAt', 'updatedAt', 'firstPublishedAt', 'publishedAt', 'analysisStateHistory']:
            if analysis.get(item):
                analysis.pop(item)

          change = False
          if analysis['analysisType']['name'] == 'consensus_sequence':
            # if 'fasta_header_name' in analysis['sample_collection']:
            #   fasta_header_name = analysis['sample_collection']['fasta_header_name']
            # else:
            #   fasta_header_name = analysis['sample_collection']['isolate']
            
            # # database_identifiers 
            # database_identifiers = analysis['database_identifiers']
            # fname = 'GISAID accession'
            # old_value = database_identifiers[field_update.get(fname)]            
            # if fname in annotation[sampleId]:  
            #   if not annotation[sampleId][fname] == old_value:
            #     change = True
            #     database_identifiers[field_update.get(fname)] = annotation[sampleId][fname]
            #     # print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: sample_collection_date is updated from \'{old_value}\' to \'{sample_collection['sample_collection_date']}\'")
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{annotation[sampleId][fname]}\'")
            #   else: 
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is NOT updated because updating value is the same as the existing one\'")


            # # host section
            # host = analysis['host']

            # for fname in ['host age', 'host age unit', 'host age bin']:
            #   old_value = host[field_update.get(fname)] 
            #   if fname in annotation[sampleId]:
                # if not annotation[sampleId][fname] == old_value:
                #   change = True
                #   host[field_update.get(fname)] = annotation[sampleId][fname]
                #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{annotation[sampleId][fname]}\'")
                # else:
                #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} has the same new value: \'{annotation[sampleId][fname]}\' as the old one: \'{old_value}\'")
            
            # # if sampleId in annotation:
            # #   old_value = host['host_age_bin']
            # #   if not old_value == annotation[sampleId]['host age bin-CURRENT in VirusSeq_2022-01-14']:
            # #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'old_host_age_bin\' is \'{old_value}' not \'{annotation[sampleId]['host age bin-CURRENT in VirusSeq_2022-01-14']}\'")

            # #   host['host_age_bin'] = annotation[sampleId]['host age bin-CHANGE TO']
            # #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age_bin' is updated from \'{old_value}\' to \'{host['host_age_bin']}\'")
            
            # if host['host_age'] in ["Not Applicable", "Missing", "Not Collected", "Not Provided", "Restricted Access"]:
            #   old_value = host['host_age']
            #   host['host_age'] = None
            #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age' is further updated from \'{old_value}\' to \'{host['host_age']}\'")
              
            #   null_reason_old_value = host['host_age_null_reason']
            #   if not null_reason_old_value == old_value:
            #     host['host_age_null_reason'] = old_value
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age_null_reason' is updated from \'{null_reason_old_value}\' to \'{host['host_age_null_reason']}\'")

       

            # if host['host_age_bin'] == '90 - 99':
            #   old_value = host['host_age_bin']            
            #   host['host_age_bin'] = "90+"
            #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age_bin' is further updated from \'{old_value}\' to \'{host['host_age_bin']}\'")
              
            #   old_value = host['host_age_null_reason']
            #   if not old_value == "Restricted Access":
            #     host['host_age_null_reason'] = "Restricted Access"
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age_null_reason' is updated from \'{old_value}\' to \'Restricted Access\'")

            #   old_value = host['host_age']
            #   if not old_value == None:
            #     host['host_age'] = None
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\t'host_age' is updated from \'{old_value}\' to \'{host['host_age']}\'")
              

            # # experiment section
            # experiment = analysis['experiment']
            # for fname in ['purpose of sequencing']:
            #   old_value = experiment[field_update.get(fname)] 
            #   if fname in annotation[sampleId]:
            #     if not old_value == annotation[sampleId][fname]:
            #       change = True
            #       experiment[field_update.get(fname)] = annotation[sampleId][fname]
            #       print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{annotation[sampleId][fname]}\'")
            #     else: 
            #       print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is NOT updated because updating value is the same as the existing one\'")
            # experiment.pop('library_id', None)
            # experiment.pop('sequencing_date', None)
            # if not 'sequencing_protocol' in experiment:
            #   seq_protocol_name = experiment.pop('sequencing_protocol_name', None) 
            #   experiment['sequencing_protocol'] = seq_protocol_name

            # # lineage_analysis section
            # analysis.pop('lineage_analysis', None)
            
            # sample_collection
            sample_collection = analysis['sample_collection']
            fname = 'purpose of sampling'
            old_value = sample_collection[field_update.get(fname)]            
            if fname in annotation[sampleId]:  
              if not annotation[sampleId][fname] == old_value:
                change = True
                sample_collection[field_update.get(fname)] = annotation[sampleId][fname]
                print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{annotation[sampleId][fname]}\'")
              else: 
                print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is NOT updated because updating value is the same as the existing one\'")


            # sample_collection['fasta_header_name'] = sample_collection['isolate']
            # sample_collection.pop('geo_loc_city', None)
            # sample_collection.pop('sample_collection_date_precision', None)
            # sample_collection.pop('nml_submitted_specimen_type', None)
            # if sample_collection['sample_collection_date']:
            #   sample_collection['sample_collection_date_null_reason'] = None
            # else:
            #   sample_collection['sample_collection_date_null_reason'] = "Not Provided"
            
            # if sample_collection['sample_collected_by'] in ["Kingston Health Sciences Centre and Queen's University", "Queen’s University / Kingston Health Sciences Centre"]:
            #   sample_collection['sample_collected_by'] = "Queen's University / Kingston Health Sciences Centre"
            # elif sample_collection['sample_collected_by'] == "LSPQ":
            #   sample_collection['sample_collected_by'] = "Laboratoire de santé publique du Québec (LSPQ)"
            # else:
            #   pass

            # # sequence_analysis
            # sequence_analysis = analysis['sequence_analysis']
            # for fname in ['raw sequence data processing method', 'dehosting method', 'consensus sequence software name', 'consensus sequence software version', 'bioinformatics protocol']: 
            #   old_value = sequence_analysis[field_update.get(fname)]
            #   if fname in annotation[sampleId] and not old_value == annotation[sampleId][fname]:
            #     sequence_analysis[field_update.get(fname)] = annotation[sampleId][fname]
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{annotation[sampleId][fname]}\'")


            # sequence_analysis['metrics'].pop('Ns_per_100kbp', None)
            # sequence_analysis['metrics'].pop('consensus_genome_length', None)
            
            # if sequence_analysis['consensus_sequence_software_name'] == "Not Provided":
            #   if fasta_header_name in annotation and annotation[fasta_header_name]['consensus sequence software name'] is not None:
            #     sequence_analysis['consensus_sequence_software_name'] = annotation[fasta_header_name]['consensus sequence software name']
            #     sequence_analysis['consensus_sequence_software_version'] = annotation[fasta_header_name]['consensus sequence software version']
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'consensus_sequence_software_name\' is updated to \'{sequence_analysis['consensus_sequence_software_name']}\'")
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'consensus_sequence_software_version\' is updated to \'{sequence_analysis['consensus_sequence_software_version']}\'")

            # if isinstance(sequence_analysis['consensus_sequence_software_version'], float):
            #   software_version = sequence_analysis['consensus_sequence_software_version']
            #   sequence_analysis['consensus_sequence_software_version'] = str(software_version)
            #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'consensus_sequence_software_version\' type is updated from float to string")

            # if sequence_analysis.get('metrics'):
            #   metrics = sequence_analysis['metrics']
            #   if 'depth_of_coverage' in metrics and metrics['depth_of_coverage'] is None:
            #     metrics['depth_of_coverage'] = 'Not Provided'
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'depth_of_coverage\' is updated to \'Not Provided\'")
            #   if 'breadth_of_coverage' in metrics and metrics['breadth_of_coverage'] is None:
            #     metrics['breadth_of_coverage'] = 'Not Provided'
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'breadth_of_coverage\' is updated to \'Not Provided\'")

            # # pathogen_diagnostic_testing
            # pathogen_diagnostic_testing = analysis['pathogen_diagnostic_testing']
            # for fname in ['gene name', 'diagnostic pcr Ct value', 'diagnostic pcr Ct value null reason']: 
            #   old_value = pathogen_diagnostic_testing[field_update.get(fname)]
            #   if fname == 'diagnostic pcr Ct value' and annotation[sampleId][fname]: 
            #     new_value = float(annotation[sampleId][fname]) 
            #   else: 
            #     new_value = annotation[sampleId][fname]
            #   if annotation[sampleId].get(fname) and not old_value == new_value:
            #     pathogen_diagnostic_testing[field_update.get(fname)] = new_value
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: {field_update.get(fname)} is updated from \'{old_value}\' to \'{new_value}\'")

            # analysis['pathogen_diagnostic_testing'] = {
            #   "gene_name": "Not Provided",
            #   "diagnostic_pcr_ct_value": None
            # } 

            # gene_name = annotation[fasta_header_name]['gene name'] if fasta_header_name in annotation else 'Not Provided'
            # if gene_name is None:
            #   gene_name = 'Not Provided'
            # diagnostic_pcr_ct_value = annotation[fasta_header_name]['diagnostic pcr Ct value'] if fasta_header_name in annotation else None
            # if 'pathogen_diagnostic_testing' in analysis:
            #   pathogen_diagnostic_testing = analysis['pathogen_diagnostic_testing']
            #   if pathogen_diagnostic_testing['gene_name'] == "Not Provided" and not gene_name == "Not Provided":
            #     pathogen_diagnostic_testing['gene_name'] = gene_name
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'gene_name\' is updated to \'{gene_name}\'")
            #   if pathogen_diagnostic_testing['diagnostic_pcr_ct_value'] is None and diagnostic_pcr_ct_value is not None:
            #     pathogen_diagnostic_testing['diagnostic_pcr_ct_value'] = diagnostic_pcr_ct_value
            #     print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'diagnostic_pcr_ct_value\' is updated to \'{diagnostic_pcr_ct_value}\'")
            
            # old_value = pathogen_diagnostic_testing['diagnostic_pcr_ct_value_null_reason']
            # if pathogen_diagnostic_testing['diagnostic_pcr_ct_value'] and old_value:
            #   pathogen_diagnostic_testing['diagnostic_pcr_ct_value_null_reason'] = None
            #   print(f"{analysis['studyId']}:{analysis['analysisId']}\t{sampleId}\tfield: diagnostic_pcr_ct_value_null_reason is updated from \'{old_value}\' to \'None\'")

            # else:
              # analysis['pathogen_diagnostic_testing'] = {
              #   "gene_name": gene_name,
              #   "diagnostic_pcr_ct_value": diagnostic_pcr_ct_value,
              #   "diagnostic_pcr_ct_value_null_reason": "Not Provided" if diagnostic_pcr_ct_value is None else None
              # }
              # print(f"{analysis['studyId']}:{analysis['analysisId']}\t\'pathogen_diagnostic_testing\' is added with \'{gene_name}\', \'{diagnostic_pcr_ct_value}\', \'{analysis['pathogen_diagnostic_testing']['diagnostic_pcr_ct_value_null_reason']}\'")               

            if change == True: 
              fm.write(json.dumps(analysis)+"\n")
              
          else:
            pass

def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dump_path", dest="dump_path", type=str, required=True, help="path to song dump jsonl file")
    parser.add_argument("-s", dest="source_metadata_files", nargs='+', required=True, help="Source metadata files to get information for requested fields")
    parser.add_argument("-e", dest="env", type=str, help="Specify environment to prepare the migration payloads", default='dev')
    args = parser.parse_args()
    
    # process the payloads
    prep_migrate_payloads(args.dump_path, args.source_metadata_files, args.env)
    

if __name__ == "__main__":
    main()
