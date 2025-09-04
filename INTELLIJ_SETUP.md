# Configuração do IntelliJ IDEA - Guia de Imports e Formatação

Este documento explica como configurar o IntelliJ IDEA para trabalhar com Spotless e manter o código consistente.

## Configuração Automática (Projeto)

Os arquivos de configuração já estão incluídos no projeto na pasta `.idea/`:
- `.idea/codeStyles/codeStyleConfig.xml` - Configuração principal
- `.idea/inspectionProfiles/Project_Default.xml` - Perfil de inspeções

## Spotless Integration

O projeto usa **Spotless** como ferramenta principal de formatação e qualidade de código, que substitui o Checkstyle com vantagens:

### Vantagens do Spotless vs Checkstyle:
- ✅ **Auto-fix**: Corrige automaticamente a maioria dos problemas
- ✅ **Google Java Format**: Formatação consistente e moderna
- ✅ **Regras customizadas**: Flexibilidade para regras específicas
- ✅ **Menos configuração**: Menos arquivos XML complexos
- ✅ **Integração nativa**: Melhor integração com Gradle

### Comandos Spotless:
```bash
./gradlew spotlessCheck    # Verifica formatação
./gradlew spotlessApply    # Aplica correções automaticamente
./gradlew check           # Roda todos os checks (inclui Spotless)
```

## Configuração Manual (Para outros projetos)

### 1. Configurar Imports (Evitar Wildcard)

1. Abra **File → Settings** (ou **IntelliJ IDEA → Preferences** no macOS)
2. Navegue para **Editor → Code Style → Java**
3. Vá para a aba **Imports**
4. Configure as seguintes opções:

```
Class count to use import with '*': 999
Names count to use static import with '*': 999
```

5. Limpe a lista "Packages to Use Import with '*'" (deixe vazia)

### 2. Instalar Plugin Google Java Format

1. Vá para **File → Settings → Plugins**
2. Procure por "google-java-format"
3. Instale e reinicie o IntelliJ
4. Ative em **Settings → google-java-format Settings**
5. Marque "Enable google-java-format"

### 3. Configurar Formatação Automática

1. Em **Editor → Code Style → Java**
2. Configure:
   - **Right margin**: 120
   - **Wrap long lines**: ✓
   - **Tab size**: 2
   - **Indent**: 2
   - **Continuation indent**: 4

### 4. Ativar Inspeções para Imports

1. Vá para **Editor → Inspections**
2. Procure por "On demand import" e marque como **Error**
3. Procure por "Unused import" e marque como **Warning**

## Workflow Recomendado

### Antes de Commit:
1. **Ctrl+Alt+L** (Cmd+Alt+L no Mac): Reformatar código
2. **Ctrl+Alt+O** (Cmd+Alt+O no Mac): Otimizar imports
3. Execute: `./gradlew spotlessApply`
4. Execute: `./gradlew check`

### Durante Desenvolvimento:
- Configure "Actions on Save" para rodar formatação automaticamente
- Use o plugin google-java-format para formatação consistente

## Regras Implementadas no Spotless

O projeto implementa as seguintes verificações customizadas:

### 1. **No Wildcard Imports**
```
import java.util.*;  // ❌ Erro
import java.util.List;  // ✅ Correto
```

### 2. **Line Length Check**
- Máximo 120 caracteres por linha
- Exceções: comentários e URLs

### 3. **Magic Numbers Check**
- Números literais devem ser constantes
```java
int size = 42;  // ❌ Erro
private static final int DEFAULT_SIZE = 42;  // ✅ Correto
```

### 4. **Formatação Automática**
- Google Java Format
- Remoção de imports não utilizados
- Remoção de espaços em branco
- Newline no final dos arquivos

## Troubleshooting

### Se o Spotless falhar:
1. Execute `./gradlew spotlessApply` para corrigir automaticamente
2. Verifique se não há imports wildcard
3. Verifique se não há linhas muito longas
4. Verifique se não há números mágicos

### Se o IntelliJ não formatar corretamente:
1. Verifique se o plugin google-java-format está ativado
2. Execute **File → Invalidate Caches and Restart**
3. Verifique se **Use per-project settings** está ativado

## CI/CD Integration

O pipeline automaticamente:
- ✅ Roda `spotlessCheck` para verificar formatação
- ✅ Roda `spotbugsMain/Test` para análise estática
- ✅ Falha se houver violações de formatação
- ✅ Gera relatórios de qualidade de código
