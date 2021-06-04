# !/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Eva Bühlmann, BA
#
# GOAL:
# - Sample ads based on topics / source and years
# - Extract relevant text zones from sampled ads.

import os
from lxml import etree
import bz2
import json
from collections import defaultdict
import re
import random


def sample_ads(infile, outpath, sample_name, source, samplingdict, topic_dict, topic_samplingdict, zones=(60, 70),
               threshold=10, id_file="ids_sampled_ads.txt", multi_year_file=False):

    """Extract text from ads in XML format (bz2-compressed) to txt file, based on topics.

    Extracted text is stored in a jsonl-File with "id", "text" and "meta". Example:
    {"id": "sjmm-12001121020008", "text": "\nCoop\nVerkäufer/in\n(2 Jahre)\... und umfassende Ausbildung.",
    "meta": {"year": 2001, "source": "sjmm", "lang": "de"}}

    :param infile: path to XML file
    :param outpath: path to store jsonl-file with extracted ads
    :param sample_name: name of sample
    :param source: x28, adecco or sjmm

    :param samplingdict: dictionary with desired nr of ads per year (key: year, value: number)
    :param topic_dict: dictionary with topic per ad (key: ad_id, value: topic)
    :param topic_samplingdict: dictionary with desired nr of ads per topic (key: topic, value: number)

    :param zones: Set with integers which define text zones to consider. Default: zones 60 & 70.
    :param threshold: Integer, defines how many tokens/spaces around selected zones are considered. Default: 10.

    :param id_file: file with ids of extracted ads. Already extracted ads are excluded. Default: outpath/ids_sampled_ads.txt
    Format: tab separated txt-file (sample_name\tad_id\tsource\tyear\ttopic\tinfile')

    :param multi_year_file: File contains ads from several years. Default: False

    """

    parser = etree.XMLParser(remove_blank_text=True)
    extracting = True

    # only parse file if more samples for this year are needed (not relevant for sjmm-files with max. 1 file per year)
    if source != 'sjmm':
        if source == 'x28':
            year = int(re.search('ads_zoned_(\d\d\d\d)', infile).group(1))
        else:
            year = int(re.search('ads_annotated_ji_instexte_(\d\d\d\d)', infile).group(1))
        if samplingdict[year] == 0 or samplingdict['total'] == 0:
            extracting = False

    if extracting:
        # open id-file
        try:
            idfile = open(os.path.join(outpath, id_file), encoding='utf-8')
            existing_document_id_list = [line.split()[1].rstrip() for line in idfile]
            idfile.close()
        except FileNotFoundError:
            print(f'No existing id-file! New id-file created ({os.path.join(outpath,"ids_sampled_ads.txt")}) ')
            existing_document_id_list = []

        idfile = open(os.path.join(outpath, id_file), 'a', encoding='utf-8')

        # open XML-file
        xml_file = bz2.open(infile, 'rt', encoding='utf-8')

        outfile_name = f'sample_{sample_name}.jsonl'
        outfile = open(os.path.join(outpath, outfile_name), 'a', encoding='utf-8')

        tree = etree.parse(xml_file, parser)
        root = tree.getroot()

        # Max number of ads per file for x28-files.
        # Extraction stops when max is reached, in order to have ads from multiple files
        if source == 'x28':
            max_number_ads_per_file = 5

        number_of_ads_extracted = 0

        # Start extracting
        ad_list = list(root.iter('ad'))
        random.shuffle(ad_list)  # list with ads in random order

        counter = -1

        # Iterate over ads in XML-file
        for ad in ad_list:
            counter += 1

            ad_content = ad[0]
            year = int(ad.get('year'))
            ad_id = source + '-' + ad.get('id')

            ad_topic = topic_dict[ad_id]

            # if ad_topic not in topic_samplingdict:  # only consider ads from selected topics
            #     continue
            #
            # elif topic_samplingdict[ad_topic] == 0:  # already enough ads from this topic
            #     continue

            # not a relevant topic or already enough ads from this topic -->continue to next ad
            if ad_topic not in topic_samplingdict or topic_samplingdict[ad_topic] == 0:
                continue

            # stop sampling when enough ads per year/source sampled
            if samplingdict['total'] == 0:
                break

            if samplingdict[year] == 0:
                if multi_year_file:
                    continue  # Look for other years in file (only for files with multiple years)
                else:
                    break  # No need to iterate over whole file

            if source == 'x28':
                if number_of_ads_extracted >= max_number_ads_per_file:
                    break

            lang = ad_content.get('language')

            ad_id_list = []

            # only german ads are considered, exclude duplicates (based on ad_id)
            if lang == 'de' and year >= 2001:
                # if source == 'sjmm' and multi_year_file and samplingdict[year] == 0:  # don't consider add
                #     continue

                # check for duplicates WITHIN Files
                if ad_id in ad_id_list:
                    continue

                # if ad has been extracted before ->ignore it
                if ad_id in existing_document_id_list:
                    continue

                # add is selected

                else:
                    # Extract text from ad
                    extracted_text = extract_text(ad_content, zones, threshold)

                    if extracted_text:  # ad contains text from selected zones
                        # Only keep ads with ict-term from keyword-list
                        if 200 <= len(extracted_text) <= 2500:  # exclude very short and long ads
                            zone_json = {'id': ad_id, 'text': extracted_text}
                            zone_json['meta'] = {'year': year, 'source': source, 'lang': lang}
                            print(json.dumps(zone_json, ensure_ascii=False), file=outfile)

                            # Update sampling-dict
                            samplingdict[year] -= 1
                            samplingdict['total'] -= 1
                            number_of_ads_extracted += 1
                            topic_samplingdict[ad_topic] -= 1

                            # Write id of extracted ad in separate file
                            idfile.write(f'{sample_name}\t{ad_id}\t{source}\t{year}\t{ad_topic}\t{infile}\n')
        xml_file.close()


def extract_text(ad_content, zones, threshold):
    """Parse XML Element, return text belonging to selected zones +/- threshold
    :param: ad_content: lxml.etree._Element, containing ad elements
    :param: zones: text zones
    :param: threshold: threshold of token/spaces around zones"""

    # get positions of all tokens in selected zones:
    pos_selected_zones = set()
    last_position = None
    pos_selected_all = []
    for el in ad_content.iter("token", "space"):
        position = int(el.get('position'))
        if int(el.get('zone')) // 10 * 10 in zones:
            pos_selected_zones.add(position)
            last_position = position

        if len(pos_selected_zones) < 1:  # If ad doesn't contain any text from selected zones ->ignore it
            continue

        # find positions of all tokens in defined threshold around zones
        pos_selected_all = set()
        for position in pos_selected_zones:
            for i in range(threshold + 1):
                if position + i <= last_position:
                    pos_selected_all.add(position + i)
                if position - i > 0:
                    pos_selected_all.add(position - i)

    # Prepare for extraction
    pos_selected_all = sorted(pos_selected_all)
    zones_ad_string = ''

    if len(pos_selected_zones) > 1:
        # Extract text from specified zones
        for el in ad_content.iter("token", "space"):
            pos = int(el.get('position'))
            if pos in pos_selected_all:
                zones_ad_string += el.text
        return zones_ad_string

    return None


def get_topic_ids(ad_topic_file, topic_share=0.4):
    """Convert topic-jsonl.bz2 into dictionary. Key = ad-id. Value = most important topic
    (only if it has a share > parameter topic_share. Default = 0.4)
    """
    topic_dict = defaultdict(str)
    with bz2.open(ad_topic_file, 'rt', encoding='utf-8') as inf:
        for line in inf:
            json_line = json.loads(line)
            ad_id = json_line['id']
            topics = json_line['topics']
            for topic in topics:
                if topic['p'] > topic_share:
                    topic_dict[ad_id] = topic["t"]
                    break
    return topic_dict


def main():
    # Get information from topic model
    path = "C:/Users/va_bu/ba_repo/ict-terms-prodigy/topicmodeling/mallet.d"
    infile = "de-jobads.model.topic_assignments.jsonl.bz2"
    topic_dict = get_topic_ids(os.path.join(path, infile))

    ## ------- Settings SAMPLE-1 -------
    # sample_name = 'sample-1'
    # ICT_topics = [4, 21, 24, 25]
    # topic_samplingdict = {}
    # for t in ICT_topics:
    #     topic_samplingdict[t] = 25
    #
    ## ------- Settings SAMPLE-2 -------
    # sample_name = 'sample-2'
    # ICT_near_topics= [23, 30, 31, 35, 39, 71, 80, 81, 88, 92]
    # topic_samplingdict = {}
    # for t in ICT_near_topics:
    #     topic_samplingdict[t] = 20

    # ------- Settings SAMPLE-3 -------
    sample_name = 'sample-3'
    All_topics = [i for i in range(100)]
    topic_samplingdict = {}
    for t in All_topics:
        topic_samplingdict[t] = 2

    # id_file = "/mnt/storage/clwork/users/ebuehlmann/ba/data/sample/ict_sample/ict_ids_sampled_ads.txt"
    # out_path = "/mnt/storage/clwork/users/ebuehlmann/ba/data/sample/ict_sample"

    # test TODO: WIEDER LÖSCHEN
    out_path="C:/Users/va_bu/OneDrive/Dokumente/Computerlinguistik/Bachelorarbeit/Programming/Material/Inseratedaten/Sample/ict_sample/Scripttest"

    # ---------------- SAMPLE DATA -----------------

    # TODO: adjust DATA paths (path to XML Files)
    sjmm_path = "C:/Users/va_bu/switchdrive/annotated"
    adecco_path = "C:/Users/va_bu/switchdrive/annotated (2)"
    x28_path = "C:/Users/va_bu/switchdrive/x28"

    # --------- sjmm data ---------
    # TODO: adjust number of desired ads per year/total for sjmm ads
    source = 'sjmm'
    sjmm_years = [i for i in range(2001, 2020)]
    sjmm_sampling_dict = defaultdict(int)
    for y in sjmm_years:
        sjmm_sampling_dict[y] = 4  # Ads per year
    sjmm_sampling_dict['total'] = 76  # Ads total

    # sjmm --> all files are considered for sampling
    multi_filelist = ['ads_manual_annotated_5014_v5.xml.bz2', 'ads_annotated_1516_LSTM_v5.xml.bz2']
    single_filelist = ['ads_annotated_17_LSTM_v5.xml.bz2', 'ads_annotated_18_LSTM_v5.xml.bz2',
                       'ads_annotated_19_LSTM_v5.xml.bz2']

    print('Sampling sjmm ads...')

    for file in multi_filelist:
        sample_ads(os.path.join(sjmm_path, file), out_path, sample_name, source, sjmm_sampling_dict, topic_dict,
                   topic_samplingdict, multi_year_file=True)

    for file in single_filelist:
        sample_ads(os.path.join(sjmm_path, file), out_path, sample_name, source, sjmm_sampling_dict, topic_dict,
                   topic_samplingdict)

    print('sjmm ads sampled. Continue with adecco...')

    # --------- adecco data ---------
    # TODO: adjust number of desired ads per year/total for adecco ads
    source = 'adecco'
    adecco_years = [i for i in range(2015, 2021)]
    adecco_sampling_dict = defaultdict(int)
    for y in adecco_years:
        if y > 2016:
            adecco_sampling_dict[y] = 11  # Ads per year
        else:
            adecco_sampling_dict[y] = 10
    adecco_sampling_dict['total'] = 64  # Ads total

    filelist = [file for file in os.listdir(adecco_path) if file.endswith('.xml.bz2')]
    random.shuffle(filelist)  # randomly shuffle files
    for file in filelist:
        sample_ads(os.path.join(adecco_path, file), out_path, sample_name, source, adecco_sampling_dict, topic_dict,
                   topic_samplingdict)

    print('adecco ads sampled. Continue with x28...')

    # --------- x28 data---------
    # TODO: adjust number of desired ads per year/total for x28 ads
    source = 'x28'
    x28_years = [i for i in range(2014, 2019)]
    x28_sampling_dict = defaultdict(int)
    for y in x28_years:
        x28_sampling_dict[y] = 12  # Ads per year
    x28_sampling_dict['total'] = 60  # Ads total

    folderlist = [folder for folder in os.listdir(x28_path) if folder.startswith("ads_zoned")]

    # only pick 10 random files per folder:
    for folder in folderlist:
        subfolder = os.path.join(x28_path, folder)
        filelist = [file for file in os.listdir(subfolder) if file.endswith('.xml.bz2')]

        selected_files = random.sample(filelist, 10)
        for file in selected_files:
            sample_ads(os.path.join(subfolder, file), out_path, sample_name, source, x28_sampling_dict, topic_dict,
                       topic_samplingdict)

    print(f'x28 ads sampled. Sample {sample_name} finished!')

    # --------- SHUFFLING extracted ads ---------
    print("Shuffling file")
    with open(os.path.join(out_path, f'sample_{sample_name}.jsonl'), encoding='utf-8')as inf:
        with open(os.path.join(out_path, f'sample_{sample_name}_shuffled.jsonl'), 'w', encoding='utf-8') as outfile:
            lines = [json.loads(line.rstrip()) for line in inf]
            random.shuffle(lines)
            for line in lines:
                print(json.dumps(line, ensure_ascii=False), file=outfile)


if __name__ == '__main__':
    main()
