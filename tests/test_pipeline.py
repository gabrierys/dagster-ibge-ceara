import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from dagster import build_asset_context
from dagster_ceara.dagster_ceara import (
    fetch_ceara_population,
    save_ceara_csv,
    population_statistics,
    data_quality_check
)


class TestCearaPopulationPipeline:
    
    def test_fetch_ceara_population_success(self):
        
        mock_response_data = [
            {"NN": "Nível Territorial", "MN": "Unidade de Medida", "V": "Valor", "D1N": "Município", "D2N": "Variável", "D3N": "Ano"},
            {"NN": "Município", "MN": "Pessoas", "V": "100000", "D1N": "Fortaleza - CE", "D2N": "População residente estimada", "D3N": "2024"},
            {"NN": "Município", "MN": "Pessoas", "V": "50000", "D1N": "Caucaia - CE", "D2N": "População residente estimada", "D3N": "2024"}
        ]
        with patch('requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = mock_response_data
            mock_get.return_value = mock_response
            context = build_asset_context()
            result = fetch_ceara_population(context)
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert 'municipio' in result.columns
            assert 'populacao' in result.columns
            assert 'ano' in result.columns
            assert 'data_extracao' in result.columns
            assert result['municipio'].iloc[0] == 'Fortaleza - CE'
            assert result['populacao'].iloc[0] == 100000
    
    def test_population_statistics(self):
        
        test_data = pd.DataFrame({
            'municipio': ['Fortaleza - CE', 'Caucaia - CE', 'Sobral - CE'],
            'populacao': [2700000, 400000, 200000],
            'ano': ['2024', '2024', '2024'],
            'data_extracao': ['2024-01-01', '2024-01-01', '2024-01-01']
        })
        context = build_asset_context()
        result = population_statistics(context, test_data)
        assert isinstance(result, dict)
        assert result['total_municipios'] == 3
        assert result['populacao_total'] == 3300000
        assert result['municipio_mais_populoso'] == 'Fortaleza - CE'
        assert result['municipio_menos_populoso'] == 'Sobral - CE'
        assert 'populacao_media' in result
        assert 'populacao_mediana' in result
        assert 'desvio_padrao' in result
    
    def test_data_quality_check_excellent(self):
        
        test_data = pd.DataFrame({
            'municipio': ['Fortaleza - CE', 'Caucaia - CE', 'Sobral - CE'],
            'populacao': [2700000, 400000, 200000],
            'ano': ['2024', '2024', '2024'],
            'data_extracao': ['2024-01-01', '2024-01-01', '2024-01-01']
        })
        context = build_asset_context()
        result = data_quality_check(context, test_data)
        assert isinstance(result, dict)
        assert result['total_registros'] == 3
        assert result['registros_completos'] == 3
        assert result['valores_duplicados'] == 0
        assert result['populacao_zero_negativa'] == 0
        assert result['quality_score'] == 100.0
        assert result['status_qualidade'] == 'EXCELENTE'
    
    def test_data_quality_check_with_issues(self):
        
        test_data = pd.DataFrame({
            'municipio': ['Fortaleza - CE', 'Caucaia - CE', 'Sobral - CE', 'Sobral - CE'],
            'populacao': [2700000, 400000, 0, 200000],
            'ano': ['2024', '2024', '2024', '2024'],
            'data_extracao': ['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01']
        })
        context = build_asset_context()
        result = data_quality_check(context, test_data)
        assert isinstance(result, dict)
        assert result['total_registros'] == 4
        assert result['valores_duplicados'] == 1
        assert result['populacao_zero_negativa'] == 1
        assert result['quality_score'] < 100.0
        assert result['status_qualidade'] in ['BOA', 'REGULAR', 'RUIM']
    
    def test_save_ceara_csv(self):
        
        test_data = pd.DataFrame({
            'municipio': ['Fortaleza - CE', 'Caucaia - CE'],
            'populacao': [2700000, 400000],
            'ano': ['2024', '2024'],
            'data_extracao': ['2024-01-01', '2024-01-01']
        })
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            context = build_asset_context()
            save_ceara_csv(context, test_data)
            mock_to_csv.assert_called_once_with(
                "populacao_municipios_ceara.csv",
                index=False,
                encoding='utf-8'
            )


@pytest.fixture
def sample_population_data():
    return pd.DataFrame({
        'municipio': [
            'Fortaleza - CE',
            'Caucaia - CE', 
            'Sobral - CE',
            'Maracanaú - CE',
            'Crato - CE'
        ],
        'populacao': [2700000, 400000, 200000, 220000, 130000],
        'ano': ['2024'] * 5,
        'data_extracao': ['2024-01-01'] * 5
    })


class TestDataValidation:
    
    def test_population_values_are_positive(self, sample_population_data):
        assert (sample_population_data['populacao'] > 0).all()
    
    def test_no_duplicate_municipalities(self, sample_population_data):
        duplicates = sample_population_data['municipio'].duplicated().sum()
        assert duplicates == 0
    
    def test_required_columns_present(self, sample_population_data):
        required_columns = ['municipio', 'populacao', 'ano', 'data_extracao']
        for col in required_columns:
            assert col in sample_population_data.columns
    
    def test_population_data_types(self, sample_population_data):
        assert sample_population_data['populacao'].dtype in ['int64', 'float64']
        assert sample_population_data['municipio'].dtype == 'object'
        assert sample_population_data['ano'].dtype == 'object'


if __name__ == "__main__":
    pytest.main([__file__])
