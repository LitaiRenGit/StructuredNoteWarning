# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 14:55:38 2020

@author: renli
"""

if __name__ == "__main__":
    import warnings
    import StructuredNote as SN
    import ReadFiles as RF
    warnings.filterwarnings('ignore')
    structurenotes,data=RF.read_excel(r'../Data\StructuredNote_template3.xlsx')
    # structurenotes=RF.read_db()
    SN.update_structurenotes(structurenotes)
    # RF.to_excel(r'../Data\output_template.xlsx',structurenotes)
    # for i,sn in enumerate(structurenotes):
    #     print('{}号凭证预警信息：'.format(i+1))
    #     sn.print_warning()
    # RF.to_db(structurenotes,auto_key=False)
    RF.price_to_db(data)
    
    # RF.conn.close()