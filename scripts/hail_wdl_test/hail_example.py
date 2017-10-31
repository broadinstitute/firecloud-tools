from hail import *
import argparse

def summarizeVDS(self):
    '''
    Generate a summary of VDS including
    total nIndels multiallelics MAF01 MAF05
    '''
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
    parser = argparse.ArgumentParser(description='Example code for sample QC')
    parser.add_argument('--inputVds', help = 'input VDS file')
    parser.add_argument('--outputVds', help = 'output VDS file')
    parser.add_argument('--annot', help = 'Sample annotation file')
    parser.add_argument('--qcResults', help = 'Output to sample QC results')
    args = parser.parse_args()

    hc = HailContext()
    vds = hc.read(args.inputVds)
    # add annotations
    table = hc.import_table(args.annot, impute=True).key_by('Sample')
    vds = (vds.annotate_samples_table(table, root='sa.pheno').sample_qc())
    vds.variant_qc().summarizeVDS()
    vds.export_samples(args.qcResults, 'Samples = s, sa.qc.*')
    vds.write(args.outputVds, overwrite = True)