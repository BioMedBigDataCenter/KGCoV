import rootpath
rootpath.append()

from src.utils.step import Step
from src.dicts.contient import build as continent_dict
from src.text.kaggle_new_caseid import build as kaggle_new_caseid
from src.text.wiki_merge_new_caseid import build as wiki_new_caseid
from src.text.manual_new_caseid import build as sk_new_caseid
from src.text.text_merge_new import build as text_merge_all
from src.text.text_new_qc import build as text_qc
from src.text.aticle_qccase_merge import build as article_qc
from src.text.text_percentile import build as node_new_case
from src.text.clinical import build as clinicalinfo_symptom
from src.text.contact import build as contactinfo
from src.text.travel import build as travelinfo
from src.meta.meta_txt_to_table import build as meta_extract_table
from src.meta.meta_newvigtk_qc import build as meta_qc
from src.match.case_genome_match import build as case_genome_match
from src.match.LINK_MATCH_CaseGenome import build as matched_casegenome
from src.variant.NODE_Variant import build as variant
from src.dicts.proteins import build as protein
from src.dicts.sym_feature_dict import build as sars2
from src.variant.feature import build as feature
from src.match.LINK_VARIANT_GenomeVariant import build as match_genomevariant
from src.match.LINK_AFFECT_VariantProtein import build as match_variantprotein
from src.variant.link_feature_proteinfeature import build as match_proteinfeature
from src.variant.link_variant_feature import build as match_variantfeature
from src.location.NODE_Location import build as node_location
from src.location.DATA_Country import build as matched_country
from src.clade.event_genome import build as event_genome
from src.text.create_sample import build as create_sample
from src.article_data.article_case import build as article_case
from src.article_data.article_genome import build as articel_genome
from src.article_data.case_genome import build as case_genome
from src.article_data.article_variant import build as article_variant


step = Step('KGCOV data')

step('create conyinent dict')
continent_dict()

step('case_merge')
kaggle_new_caseid()
wiki_new_caseid()
sk_new_caseid()
text_merge_all()

step('case_qc,create NODE_Case')
text_qc()
article_qc()
node_new_case()

step('create LINK_CaseClinicalinfo and node_symptom')
clinicalinfo_symptom()

step('create LINK_CaseContactinfo')
contactinfo()

step('create LINK_CaseTravelinfo')
travelinfo()

step('meta_extract,meta_qc,create NODE_Genome')
meta_extract_table()
meta_qc()

step('start match case and genome by rules')
case_genome_match()

step('create Link_MATCH_CaseGenome')
matched_casegenome()

step('create NODE_Variant')
variant()

step('create NODE_Protein')
protein()

step('create sars2_dict')
sars2()

step('create NODE_feature')
feature()

step('create LINK_VARIANT_GenomeVariant')
match_genomevariant()

step('create LINK_AFFECT_VariantProtein')
match_variantprotein()

step('create LINK_FEATURE_ProteinFeature')
match_proteinfeature()

step('create LINK_FUNCTION_VariantFeature')
match_variantfeature()

step('create node_location')
node_location()
 
step('create matched country')
matched_country()

step('create node_event and link_event_genome')
event_genome()

step('create sample')
create_sample()


step('create article data')
article_case()
articel_genome()
case_genome()
article_variant()



step.finish()












