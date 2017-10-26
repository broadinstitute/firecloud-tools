from hail import *
import argparse

def summarizeVDS(self, split = True):
    '''
    Generate a summary of VDS including
    total nIndels multiallelics MAF01 MAF05
    '''
    if split:
        queries = [
            'variants.count()',
            'variants.filter(v => v.altAllele.isSNP()).count()',
            'variants.filter(v => v.altAllele.isIndel()).count()',
            'variants.filter(v => va.wasSplit).count()',
            'variants.filter(v => va.qc.AF > 0.01 && va.qc.AF < 0.99).count()',
            'variants.filter(v => va.qc.AF > 0.05 && va.qc.AF < 0.95).count()',
            'variants.filter(v => v.contig == "chrX").count()',
            'variants.filter(v => v.altAllele.isSNP() && v.contig == "chrX").count()',
            'variants.filter(v => v.altAllele.isIndel() && v.contig == "chrX").count()',
            'variants.filter(v => va.wasSplit && v.contig == "chrX").count()',
            'variants.filter(v => va.qc.AF > 0.01 && va.qc.AF < 0.99 && v.contig == "chrX").count()',
            'variants.filter(v => va.qc.AF > 0.05 && va.qc.AF < 0.95 && v.contig == "chrX").count()'
        ]
    else:
        queries = [
            'variants.count()',
            'variants.filter(v => v.isBiallelic).count()',
            'variants.filter(v => va.info.AF.sum() > 0.01 && va.info.AF.sum() < 0.99).count()',
            'variants.filter(v => va.info.AF.sum() > 0.05 && va.info.AF.sum() < 0.95).count()',
            'variants.filter(v => v.contig == "chrX").count()',
            'variants.filter(v => v.isBiallelic() && v.contig == "chrX").count()',
            'variants.filter(v => va.info.AF.sum() > 0.01 && va.info.AF.sum() < 0.99 && v.contig == "chrX").count()',
            'variants.filter(v => va.info.AF.sum() > 0.05 && va.info.AF.sum() < 0.95 && v.contig == "chrX").count()'
        ]
    print(self.query_variants(queries))
    return(self)

VariantDataset.summarizeVDS = summarizeVDS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--inputVds', dest='inputVds',
                    help='')
    parser.add_argument('--inputAnnot', dest='inputAnnot', 
                    help='')
    parser.add_argument('--output', dest='output', 
                    help='')
    parser.add_argument('--qcResults', dest='qcResults', 
                    help='')

    args = parser.parse_args()

    hc = HailContext()
    input = args.inputVds
    annot = args.inputAnnot

    output = args.output
    qc_results = args.qcResults
    
    vds = hc.read(input)
    vds.summarizeVDS()

    # add annotations
    table = hc.import_table(annot, impute=True).key_by('Sample')
    vds = (vds.annotate_samples_table(table,
                                     root='sa.pheno',
                                     sample_expr='Sample',
                                     config=TextTableConfig(impute=True))
        .sample_qc())
    vds.export_samples(qc_results, 'Samples = s, sa.qc.*')
    vds.write(output)