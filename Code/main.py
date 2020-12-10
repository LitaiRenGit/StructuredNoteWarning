# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 14:55:38 2020

@author: renli
"""
def update_structurenotes(structurenotes):
    import pandas as pd
    for sn in structurenotes:
        last_update=sn.profile.loc['最后更新日期']
        if pd.isna(last_update):
            sn.update()
        else:
            sn.update(last_update)
    return structurenotes

if __name__ == "__main__":
    import warnings
    import StructuredNote as SN
    import ReadFiles as RF
    warnings.filterwarnings('ignore')
    # structurenotes=RF.read_excel(r'../Data\StructuredNote_template.xlsx')
    structurenotes=RF.read_db()
    update_structurenotes(structurenotes)
    # RF.to_excel(r'../Data\output_template.xlsx',structurenotes)
    # for i,sn in enumerate(structurenotes):
    #     print('{}号凭证预警信息：'.format(i+1))
    #     sn.print_warning()
    RF.to_db(structurenotes)
    RF.conn.close()