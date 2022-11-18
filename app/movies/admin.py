from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from rangefilter.filters import DateRangeFilter

from .models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',)


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person',)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    search_fields = ('name',)


class RatingListFilter(admin.SimpleListFilter):
    """
    Фильтр для сортировки фильмов по рейтингу
    Больше или меньше 7.0
    """

    title = _('Rating filter')
    parameter_name = 'rating'

    def lookups(self, request, model_admin):
        return (
            ('High', _('High rating')),
            ('Low', _('Low rating')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'High':
            return queryset.filter(rating__gte=7.0, rating__lte=10.0)
        if self.value() == 'Low':
            return queryset.filter(rating__gte=0.0, rating__lte=6.9)


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):

    class Media:
        css = {
            'all': ('../static/admin/css/custom_admin.css', )     # Include extra css
        }

    inlines = (GenreFilmworkInline, PersonFilmworkInline,)

    # Отображение полей в списке
    list_display = ('title', 'type', 'creation_date', 'rating', 'created', 'modified', 'get_genres')
    list_prefetch_related = ('genres',)

    def get_queryset(self, request):
        queryset = (
            super()
                .get_queryset(request)
                .prefetch_related(*self.list_prefetch_related)
        )
        return queryset

    def get_genres(self, obj):
        return ', '.join([genre.name for genre in obj.genres.all()])

    get_genres.short_description = 'Жанры фильма'

    # Фильтрация в списке; добавлен свой фильтр через отдельный класс
    # и фильтр через стороннюю библиотеку
    list_filter = ('type', RatingListFilter, ('creation_date', DateRangeFilter))

    # Поиск по полям
    search_fields = ('title', 'description', 'id')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    search_fields = ('full_name',)


