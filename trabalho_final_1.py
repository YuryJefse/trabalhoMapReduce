from mrjob.job import MRJob, MRStep

class MBAtrabalhoFinal(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2),
            MRStep(mapper=self.mapper3, reducer=self.reducer3)
        ]

    def mapper1(self, _, line):
        #invoiceNo; stockCode; quantity; customerId; country
        invoiceno, stockedcode, _, _, _ = line.split(';')

        yield invoiceno, stockedcode

    def getCombinations(self, rows):

        resultCombinations = []
        
        for i in range(len(rows)):
            for j in range(i+1,len(rows)):
                resultCombinations.append((rows[i], rows[j]))
        
        return resultCombinations

    def reducer1(self, key, values):
        rows = list(values)

        rows.sort()

        combinations = set(self.getCombinations(rows))

        for item in combinations:
            yield item, 1
    
    def mapper2(self, key, value):
        yield key, value
    
    def reducer2(self, key, values):
        freq = sum(list(values))

        yield key[0], (key[1], freq)

    def mapper3(self, key, value):
        yield key, value
    
    def reducer3(self, key, values):
        yield key, sorted(values, key=lambda item: item[1], reverse=True)

if __name__ == "__main__":
    MBAtrabalhoFinal.run()