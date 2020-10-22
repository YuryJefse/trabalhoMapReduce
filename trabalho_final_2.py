from mrjob.job import MRJob, MRStep

class SegundaQuestao(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]
    
    def mapper1(self, _, line):
        #invoiceNo; stockCode; quantity; customerId; country
        _, stockedcode, _, customerId, _ = line.split(';')

        yield customerId, stockedcode

    def reducer1(self, key, values):
        yield key, list(values)

    def mapper2(self, _, values):
        rows = list(values)

        for stock in rows:
            temp = {}
            for tstock in rows:
                if tstock not in temp:
                    temp[tstock] = 0
                temp[tstock] += 1
            
            yield stock, temp

    def reducer2(self, key, values):
        rows = list(values)

        temp = {}
        for stockList in rows:
            for stock, freq in stockList.items():
                if stock not in temp:
                    temp[stock] = 0
                temp[stock] += freq

        yield key, sorted(temp.items(), key=lambda x: x[1], reverse=True)[:5]


if __name__ == "__main__":
    SegundaQuestao.run()