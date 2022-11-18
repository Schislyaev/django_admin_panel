from django.contrib.postgres.aggregates import ArrayAgg
from django.core.paginator import Paginator
from django.db.models import Case, F, Q, When
from django.http import JsonResponse
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView
from movies.models import Filmwork


class MoviesApiMixin:
    model = Filmwork
    paginate_by = 50
    http_method_names = ['get']

    def _aggregate_person(self, role):
        return ArrayAgg('roles__full_name', filter=Q(roles__personfilmwork__role=role), distinct=True)

    def get_queryset(self):
        return Filmwork.objects.prefetch_related('roles', 'genres')\
            .values('id', 'title', 'description', 'creation_date', 'rating', 'type') \
            .annotate(genres=ArrayAgg('genres__name', filter=Q(genres__name__isnull=False), distinct=True),
                      actors=self._aggregate_person('actor'),
                      directors=self._aggregate_person('director'),
                      writers=self._aggregate_person('writer'))

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView, Paginator):

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()

        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset,
            self.paginate_by
        )
        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': list(queryset)
        }
        return context


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, **kwargs):

        context = super().get_context_data(**kwargs).get('object')

        return context
