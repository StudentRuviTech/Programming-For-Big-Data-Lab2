from mrjob.job import MRJob

class WineQualityAnalysis(MRJob):

    def mapper(self, _, line):
        # Skip the header row (it contains the word "alcohol")
        if "alcohol" in line.lower():
            return

        parts = line.split(';')
        quality = parts[11].replace('"', '')
        alcohol = float(parts[10].replace('"', ''))
        yield quality, alcohol

    def reducer(self, quality, alcohol_values):
        values = list(alcohol_values)
        avg_alcohol = sum(values) / len(values)
        yield f"Quality {quality} avg alcohol", avg_alcohol

if __name__ == '__main__':
    WineQualityAnalysis.run()

