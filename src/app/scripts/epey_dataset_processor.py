import csv
import pandas as pd
from pathlib import Path
from collections import Counter

class DatasetProcessor:
    
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parents[1]
        self.raw_input = self.base_dir / 'outputs' / 'datasets' / 'raw' / 'epey_popular_phones_full.csv'
        self.output_basic = self.base_dir / 'outputs' / 'datasets' / 'processed' / 'products_basic_info.csv'
        self.output_common = self.base_dir / 'outputs' / 'datasets' / 'processed' / 'products_common_features.csv'
        self.output_ml = self.base_dir / 'outputs' / 'datasets' / 'processed' / 'products_ml_features.csv'
        self.processed_dir = self.base_dir / 'outputs' / 'datasets' / 'processed'

        self.selected_features = [
            'urun_fiyat',
            'urun_puan',
            'ekran_ekran_boyutu',
            'ekran_ekran_teknolojisi',
            'ekran_ekran_çözünürlüğü',
            'ekran_ekran_yenileme_hızı',
            'batarya_batarya_kapasitesi_tipik',
            'batarya_hızlı_şarj_gücü_maks.',
            'batarya_kablosuz_şarj',
            'kamera_kamera_çözünürlüğü',
            'kamera_optik_görüntü_sabitleyici_ois',
            'kamera_ön_kamera_çözünürlüğü',
            'temel_donanim_yonga_seti_chipset',
            'temel_donanim_bellek_ram',
            'temel_donanim_dahili_depolama',
            'temel_donanim_cpu_üretim_teknolojisi',
            'tasarim_kalınlık',
            'tasarim_ağırlık',
            'özelli̇kler_suya_dayanıklılık',
            'temel_bi̇lgi̇ler_çıkış_yılı'
        ]
    
    def create_basic_dataset(self):
        """Create basic product info dataset with name, price, and rating."""
        print(f'Reading raw data: {self.raw_input}')
        if not self.raw_input.exists():
            raise FileNotFoundError(f'Raw data not found: {self.raw_input}')
        
        df = pd.read_csv(self.raw_input)
        print(f'Total records in raw data: {len(df)}')
        
        dataset = df[['urun_ad', 'fiyat_tl', 'puan']].copy()
        dataset.columns = ['urun_ad', 'urun_fiyati', 'urun_puani']

        dataset = dataset.dropna(subset=['urun_ad', 'urun_fiyati', 'urun_puani'])
        dataset = dataset.reset_index(drop=True)
        
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        dataset.to_csv(self.output_basic, index=False)
        print(f'✓ Basic dataset saved: {self.output_basic}')
        print(f'  Records: {len(dataset)}, Columns: {list(dataset.columns)}\n')
        
        return dataset
    
    def create_common_features_dataset(self):
        """Create dataset with common features present across all products."""
        print(f'Creating common features dataset...')
        
        if not self.raw_input.exists():
            raise FileNotFoundError(f'Raw data not found: {self.raw_input}')
        
        df = pd.read_csv(self.raw_input)
        
        dataset1 = df[['urun_ad', 'fiyat_tl', 'puan']].copy()
        dataset1.columns = ['urun_ad', 'urun_fiyati', 'urun_puani']
        dataset1 = dataset1.dropna(subset=['urun_ad', 'urun_fiyati', 'urun_puani'])
        
        print(f'Matching products with raw data features...')
        
        dataset2_list = []
        for idx, row in dataset1.iterrows():
            matching_raw = df[(df['product_name'] == row['urun_ad']) & 
                             (df['product_price'] == row['fiyat_tl']) & 
                             (df['product_point'] == row['puan'])]
            
            if len(matching_raw) > 0:
                dataset2_list.append(matching_raw.iloc[0])
        
        dataset2_raw = pd.DataFrame(dataset2_list).reset_index(drop=True)
        print(f'Matched records: {len(dataset2_raw)}')
        
        print('Analyzing column coverage...')
        column_coverage = dataset2_raw.notna().sum() / len(dataset2_raw) * 100
        common_columns = column_coverage[column_coverage >= 50].index.tolist()
        
        dataset2 = dataset2_raw[['urun_ad'] + common_columns].copy()
        dataset2.columns = ['product_name'] + common_columns
        dataset2 = dataset2.reset_index(drop=True)
        
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        dataset2.to_csv(self.output_common, index=False)
        print(f'✓ Common features dataset saved: {self.output_common}')
        print(f'  Records: {len(dataset2)}, Columns: {len(dataset2.columns)}\n')
        
        return dataset2

    def generate_ml_features(self):
        """Extract and save selected ML features from common features dataset."""
        print(f'Reading: {self.output_common}')
        if not self.output_common.exists():
            raise FileNotFoundError(f'Input CSV not found: {self.output_common}')
        
        with self.output_common.open(newline='', encoding='utf-8') as inf:
            reader = csv.DictReader(inf)
            existing = [c for c in self.selected_features if c in reader.fieldnames]
            missing = [c for c in self.selected_features if c not in reader.fieldnames]
            
            if missing:
                print('Warning - these selected columns were not found and will be skipped:')
                for m in missing:
                    print('  -', m)
            
            with self.output_ml.open('w', newline='', encoding='utf-8') as outf:
                writer = csv.DictWriter(outf, fieldnames=existing)
                writer.writeheader()
                for row in reader:
                    out = {k: row.get(k, '') for k in existing}
                    writer.writerow(out)
        
        print(f'✓ Wrote: {self.output_ml} (columns: {len(existing)})\n')
        return len(existing)
    
    def check_constant_features(self):
        """Identify features with constant or near-constant values."""
        print(f'Reading: {self.output_ml}')
        
        with self.output_ml.open(newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            print('No data found')
            return
        
        print(f'Total rows: {len(rows)}\n')
        print('Columns with constant or near-constant values:\n')
        
        constant_cols = []
        for col in rows[0].keys():
            values = [row[col] for row in rows if row[col]]
            if not values:
                print(f'  • {col}: ALL EMPTY')
                constant_cols.append(col)
                continue
            
            unique = set(values)
            if len(unique) == 1:
                print(f'  • {col}: Always "{values[0]}"')
                constant_cols.append(col)
            elif len(unique) <= 2:
                counts = Counter(values)
                dom_val, dom_count = counts.most_common(1)[0]
                pct = (dom_count / len(values)) * 100
                if pct >= 95:
                    print(f'  • {col}: {pct:.0f}% "{dom_val}" (other: {set(values) - {dom_val}})')
                    constant_cols.append(col)
        
        if constant_cols:
            print(f'\n\nSuggestion: Remove these {len(constant_cols)} columns:')
            for col in constant_cols:
                print(f'  - {col}')
        else:
            print('✓ No problematic constant columns found.')
        
        return constant_cols


if __name__ == '__main__':
    processor = DatasetProcessor()
    
    print('='*70)
    print('UNIFIED DATASET PROCESSOR')
    print('='*70)
    
    print('\n[1/4] Creating basic product information dataset...')
    processor.create_basic_dataset()
    
    print('[2/4] Creating common features dataset...')
    processor.create_common_features_dataset()
    
    print('[3/4] Generating ML features...')
    processor.generate_ml_features()
    
    print('[4/4] Checking for constant features...')
    processor.check_constant_features()
    
    print('\n' + '='*70)
    print('✓ All dataset operations complete!')
    print('='*70)
