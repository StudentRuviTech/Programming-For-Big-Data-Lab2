from mrjob.job import MRJob

class WineQualityAnalysis(MRJob):

    def mapper(self, _, line):
        parts = line.split(';')
        if parts[0] != "fixed acidity":  # skip header
            quality = parts[11]
            alcohol = float(parts[10])
            yield quality, alcohol

    def reducer(self, quality, alcohol_values):
        values = list(alcohol_values)
        avg_alcohol = sum(values) / len(values)
        yield f"Quality {quality} avg alcohol", avg_alcohol

if __name__ == '__main__':
    WineQualityAnalysis.run()
