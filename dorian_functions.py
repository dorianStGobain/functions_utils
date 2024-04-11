def get_all_tag_providers():
	tags = system.tag.browse(filter={"tagType":"Provider"}).results
	a = []
	for result in tags:
		providers = str(result['fullPath'])
		a.append(providers)
	return a

def get_list_opc_tags(tag_provider):
	results = system.tag.browse(tag_provider,filter={"tagType":"AtomicTag","recursive":True,"valueSource":"opc","maxResults":500})

	tags = []
	for k,tag in enumerate(results.getResults()):
	    stringPath = str(tag['fullPath'])
	    stringPath
	    tag_opc=system.tag.getConfiguration(stringPath)[0]
	#    tag_opc
	    tags.append({
			'tagpath':stringPath,
			'opcItemPath':tag_opc['opcItemPath'],
			'opcServer':tag_opc['opcServer'],
			'value':tag['value'].value
			# 'tag_provider':tag_provider
			})
	tags = [{k:str(v) for k,v in t.items()} for t in tags]
	return tags

def get_list_of_historical_tags(path):
	def browse(t, path):
		for result in system.tag.browseHistoricalTags(path).getResults():
			t.append(result.getPath())

			if result.hasChildren():
				browse(t, result.getPath())
	historyPaths = []
	browse(historyPaths, path=path)
	tagPaths = []
	for tag in historyPaths:
		tag = "[sgg_tpl_coater]" + str(tag).split("tag:")[1]
		if system.tag.getConfiguration(tag)[0]['tagType']=='AtomicTag':
			tagPaths.append(tag)
	return tagPaths

def shorten_string(string, max_length=20):
    if len(string) <= max_length:
        return string
    else:
        return string[:max_length-3] + "..."

def get_row_dataSet_as_dict(dataset,row,columns=None):
    if columns is None:
        columns = dataset.getColumnNames()
	return {col:dataset.getValueAt(row, col) for col in columns}

def get_column_of_dataSet(dataset,column):
	return [dataset.getValueAt(row, column) for row in range(len(dataset))]

def export_to_csv(dataset,filepath):
	csvfiledata = system.dataset.toCSV(dataset)
	csvfiledata = csvfiledata.replace('"True"', '"1"')
	csvfiledata = csvfiledata.replace('"False"', '"0"')
	csvfiledata = csvfiledata.replace('"','')
	csvfiledata = csvfiledata.replace(',',';')
	system.file.writeFile(filepath, csvfiledata)

def element_wise_operator(func):
    def wrapper(s1,s2):
        if not isinstance(s2,Series):
            s2 = Series([s2]*len(s1),s1.name)
        if len(s1)==len(s2):
            return Series(func(s1,s2),s1.name)
        else:
            raise TypeError("the two series do not have the same length")
    return wrapper

class Series():
    def __init__(self,list,name=''):
        self.s = list
        self.name = name
        self.length = len(self.s)

    def rename(self,new_name):
        return Series(self.s,new_name)

    def __len__(self):
        return len(self.s)

    @element_wise_operator
    def __add__(self, other):
        """Addition (+)"""
        return [self.s[k] + other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __sub__(self, other):
        """Subtraction (-)"""
        return [self.s[k] - other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __mul__(self, other):
        """Multiplication (*)"""
        return [self.s[k]*other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __eq__(self, other):
        """Equal to (==)"""
        return [self.s[k] == other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __ne__(self, other):
        """Not equal to (!=)"""
        return [self.s[k] != other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __gt__(self, other):
        """Greater than (>)"""
        return [self.s[k] > other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __ge__(self, other):
        """Greater than or equal to (>=)"""
        return [self.s[k] >= other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __lt__(self, other):
        """Less than (<)"""
        return [self.s[k] < other.s[k] for k in range(len(self.s))]
    @element_wise_operator
    def __le__(self, other):
        """Less than or equal to (<=)"""
        return [self.s[k] <= other.s[k] for k in range(len(self.s))]

    def __repr__(self):
        print(self.name)
        lens = len(self.s)
        for k in range(min(10,lens)):
            print(self.s[k])
        if lens>10:
            print('...')
        return ""

    def __getitem__(self,l):
        if isinstance(l,int):
            return self.s[l]
        else:
            return Series(self.s[l],self.name)

class DataFrame():
    def __init__(self,dataset):
        self.df = dataset
        self.columns = list(self.df.getColumnNames())
        self.shape = [len(self.df),len(self.columns)]

    def _get_item_simple(self,key):
        return self.df.getValueAt(i, j)

    def _getitem_col(self,col):
        return Series([self.df.getValueAt(row, col) for row in range(len(self.df))],col)

    def _getitem_row(self,row):
        return [self.df.getValueAt(row, col) for col in range(len(self.columns))]

    def _getitems_(self,rows=None,cols=None):
        if cols is None:
            cols=self.columns
        if rows is None:
            rows = range(len(self.df))
        datas = []
        for row in rows:
            datas.append([self.df.getValueAt(row, col) for col in cols])
        if isinstance(cols[0],int):
            cols = [self.columns[k] for k in cols]
        return DataFrame(system.dataset.toPyDataSet(system.dataset.toDataSet(cols,datas)))

    def _filter_on_bool(self,filter):
        list_true = [k for k,v in enumerate(filter) if v==True]
        return self._getitems_(list_true)

    def __getitem__(self, key):
        if isinstance(key,str):
            col = key
            return self._getitem_col(col)
        else:
            return self._getitems_(key[0],key[1])

    def loc(self,key):
        if isinstance(key,int):
            key=[key]
        return self._getitems_(key)

    def __repr__(self):
        print("".join([shorten_string(str(x)).ljust(23) for x in self.columns]))
        lens=len(self.df)
        for k in range(min(5,lens)):
            row = self._getitem_row(k)
            print("".join([shorten_string(str(x)).ljust(23) for x in row]))
        if lens>10:
            print('...')
        return ""

    def to_csv(self,filepath):
        export_to_csv(self.df,filepath)
